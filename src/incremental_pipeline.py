from prefect import task, Flow, Parameter
from prefect.run_configs import LocalRun
from prefect.executors import LocalExecutor
from typing import Tuple
from indra import influence_transform, metadata_transfrom, evidence_transform, get_wm, IndraAPI
import os
from elastic import Elastic
from dart import document_transform, get_CDRs


@task(log_stdout=True)
def read_environment_variables() -> Tuple[str, str, str, str, str, str, str, str]:
    # Environment
    INDRA_HOST = os.environ.get("INDRA_HOST")  # "http://wm.indra.bio/"

    # Resources like geolocation and corpus, generally speaking source/target will be the same
    SOURCE_ES_HOST = os.environ.get("SOURCE_ES_HOST")
    SOURCE_ES_PORT = os.environ.get("SOURCE_ES_PORT")

    # Where to index documents
    TARGET_ES_HOST = os.environ.get("TARGET_ES_HOST")
    TARGET_ES_PORT = os.environ.get("TARGET_ES_PORT")
    DART_HOST = os.environ.get("DART_HOST")
    DART_USER = os.environ.get("DART_USER")
    DART_PASS = os.environ.get("DART_PASS")

    return (
        INDRA_HOST,
        SOURCE_ES_HOST,
        SOURCE_ES_PORT,
        TARGET_ES_HOST,
        TARGET_ES_PORT,
        DART_HOST,
        DART_USER,
        DART_PASS
    )


@task(log_stdout=True)
def print_inputs(
    SOURCE_ES_HOST,
    SOURCE_ES_PORT,
    TARGET_ES_HOST,
    TARGET_ES_PORT,
    INDRA_HOST,
    PROJECT_EXTENSION_ID
):
    # 0. Print out input constants
    print(f"Source Elastic: {SOURCE_ES_HOST}:{SOURCE_ES_PORT}")
    print(f"Target Elastic: {TARGET_ES_HOST}:{TARGET_ES_PORT}")
    print(f"INDRA: {INDRA_HOST}")
    print(f"PROJECT_EXTENSION_ID: {PROJECT_EXTENSION_ID}")

    if (SOURCE_ES_HOST is None or
            SOURCE_ES_PORT is None or
            TARGET_ES_HOST is None or
            TARGET_ES_PORT is None or
            INDRA_HOST is None):
        raise ValueError(
            "Missing required environment variables. Ensure that you've run `source ./.incremental_assembly_secrets.sh`.")


@task(log_stdout=True)
def fetch_project_extension(project_extension_id, SOURCE_ES_HOST, SOURCE_ES_PORT) -> Tuple[str, list]:
    source_es = Elastic(SOURCE_ES_HOST, SOURCE_ES_PORT)
    # 1. Fetch project-extension from source-es by id
    print("Fetching project-extension")
    extension = source_es.term_query(
        "project-extension", "id", project_extension_id)
    project_id = extension["project_id"]
    records = extension["records"]
    print(
        f"Found extension with {len(records)} records for project: {project_extension_id}")
    return (project_id, records)


@task(log_stdout=True)
def process_cdrs(records, DART_HOST, DART_USER, DART_PASS, TARGET_ES_HOST, TARGET_ES_PORT):

    target_es = Elastic(TARGET_ES_HOST, TARGET_ES_PORT)
    # 3. Process CDR
    doc_ids = []
    for record in records:
        doc_ids.append(record["document_id"])
    doc_ids = list(set(doc_ids))

    cdrs = get_CDRs(DART_HOST, DART_USER, DART_PASS, doc_ids)
    counter = 0
    es_buffer = []
    for cdr in cdrs:
        es_buffer.append(document_transform(cdr))
        counter = counter + 1
        if counter % 500 == 0:
            print(f"\tIndexing ... {counter}")
            target_es.bulk_write('corpus', es_buffer)
            es_buffer = []
    if len(es_buffer) > 0:
        target_es.bulk_write('corpus', es_buffer)


@task(log_stdout=True)
def request_reassembly(project_id, records, INDRA_HOST):
    # 4. Send request to INDRA for reassembly
    print("Sending project-extension request to INDRA for reassembly")
    indra = IndraAPI(INDRA_HOST)
    response = indra.add_project_records(project_id, records)
    return response


@task(log_stdout=True)
def apply_reassembly_to_es(
    response,
    project_id,
    SOURCE_ES_HOST,
    SOURCE_ES_PORT,
    TARGET_ES_HOST,
    TARGET_ES_PORT
):
    source_es = Elastic(SOURCE_ES_HOST, SOURCE_ES_PORT)
    target_es = Elastic(TARGET_ES_HOST, TARGET_ES_PORT)
    # 5. Parse INDRA response
    new_stmts = response["new_stmts"]
    new_evidence = response["new_evidence"]
    new_refinements = response["new_refinements"]
    beliefs = response["beliefs"]
    print("=" * 50)
    print(f"{len(new_stmts)} new statements.")
    print(f"{len(new_evidence)} new pieces of evidence.")
    print(f"{len(new_refinements)} new refinements.")
    print(f"{len(beliefs)} new belief scores.")
    print("=" * 50)
    # 6. Pivot "new_stmts" into array of INDRA statements and join with "beliefs",
    #   transform and index new statements to project index
    counter = 0
    es_buffer = []
    for statement in new_stmts.values():
        matches_hash = str(statement["matches_hash"])
        statement["evidence"] = new_evidence.get(matches_hash)
        statement["belief"] = beliefs.get(matches_hash)
        result = influence_transform(statement, source_es)
        counter = counter + 1
        es_buffer.append(result)
        if counter % 500 == 0:
            print(f"\tIndexing ... {counter}")
            target_es.bulk_write(project_id, es_buffer)
            es_buffer = []
    if len(es_buffer) > 0:
        print(f"\tIndexing ... {counter}")
        target_es.bulk_write(project_id, es_buffer)
        es_buffer = []
    # 7. Merge new evidence
    # Not very efficient, should do batch queries and partial fetches + update
    print("")
    print("Processing updated statements")
    update_buffer = []
    for key, evidence in new_evidence.items():
        if key not in new_stmts:
            stmt = source_es.term_query(project_id, "matches_hash", key)
            if stmt == None:
                continue

            for ev in evidence:
                stmt["evidence"].append(evidence_transform(ev, source_es))

            stmt["wm"] = get_wm(stmt["evidence"], stmt["subj"], stmt["obj"])
            update_buffer.append(stmt)
    if len(update_buffer) > 0:
        print(f"\tIndexing ... {len(update_buffer)}")
        target_es.bulk_write(project_id, update_buffer)


@task(log_stdout=True)
def mark_completed(project_id):
    print(f"Updated statements for project {project_id}.")


with Flow("incremental assembly", run_config=LocalRun(labels=["non-dask"])) as flow:
    PROJECT_EXTENSION_ID = Parameter("PROJECT_EXTENSION_ID")

    if PROJECT_EXTENSION_ID is None:
        raise ValueError("Missing parameter PROJECT_EXTENSION_ID")

    (
        INDRA_HOST,
        SOURCE_ES_HOST,
        SOURCE_ES_PORT,
        TARGET_ES_HOST,
        TARGET_ES_PORT,
        DART_HOST,
        DART_USER,
        DART_PASS
    ) = read_environment_variables()
    print_inputs(
        SOURCE_ES_HOST,
        SOURCE_ES_PORT,
        TARGET_ES_HOST,
        TARGET_ES_PORT,
        INDRA_HOST,
        PROJECT_EXTENSION_ID
    )
    (project_id, records) = fetch_project_extension(
        PROJECT_EXTENSION_ID, SOURCE_ES_HOST, SOURCE_ES_PORT)
    process_cdrs(
        records,
        DART_HOST,
        DART_USER,
        DART_PASS,
        TARGET_ES_HOST,
        TARGET_ES_PORT
    )
    response = request_reassembly(project_id, records, INDRA_HOST)
    apply_reassembly_to_es(
        response,
        project_id,
        SOURCE_ES_HOST,
        SOURCE_ES_PORT,
        TARGET_ES_HOST,
        TARGET_ES_PORT
    )
    mark_completed(project_id)

# ===========================================================

# Set this to False to run the flow locally, or
# set it to True to register the flow with the Prefect server
# to be run later on an agent.
should_register = True


if (should_register):
    flow.register(project_name="project")
else:
    PROJECT_EXTENSION_ID = os.environ.get("PROJECT_EXTENSION_ID")  # "http://wm.indra.bio/"
    if PROJECT_EXTENSION_ID is None:
        raise ValueError("Missing required environment variable PROJECT_EXTENSION_ID")

    state = flow.run(
        executor=LocalExecutor(),
        parameters={
            "PROJECT_EXTENSION_ID": PROJECT_EXTENSION_ID
        }
    )

    assert state.is_successful()
