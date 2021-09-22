from prefect import task, Flow, Parameter
from prefect.run_configs import LocalRun
from prefect.executors import LocalExecutor
from typing import Tuple
from indra import influence_transform, metadata_transfrom, evidence_transform, get_wm, IndraAPI
from curation_recommendation import CurationRecommendationAPI
import os
from elastic import Elastic
from dart import document_transform, get_CDRs
from utils import epoch_millis


@task(log_stdout=True)
def read_environment_variables() -> Tuple[str, str, str, str, str, str, str]:
    # Environment
    INDRA_HOST = os.environ.get("INDRA_HOST")  # "http://wm.indra.bio/"

    # Resources like geolocation and corpus, generally speaking source/target will be the same
    SOURCE_ES = os.environ.get("SOURCE_ES")

    # Where to index documents
    TARGET_ES= os.environ.get("TARGET_ES")

    DART_HOST = os.environ.get("DART_HOST")
    DART_USER = os.environ.get("DART_USER")
    DART_PASS = os.environ.get("DART_PASS")

    CURATION_HOST = os.environ.get("CURATION_HOST")

    return (
        INDRA_HOST,
        SOURCE_ES,
        TARGET_ES,
        DART_HOST,
        DART_USER,
        DART_PASS,
        CURATION_HOST
    )


@task(log_stdout=True)
def print_inputs(
    SOURCE_ES,
    TARGET_ES,
    INDRA_HOST,
    ASSEMBLY_REQUEST_ID
):
    # 0. Print out input constants
    print(f"Source Elastic: {SOURCE_ES}")
    print(f"Target Elastic: {TARGET_ES}")
    print(f"INDRA: {INDRA_HOST}")
    print(f"ASSEMBLY_REQUEST_ID: {ASSEMBLY_REQUEST_ID}")

    if (SOURCE_ES is None or TARGET_ES is None or INDRA_HOST is None):
        raise ValueError(
            "Missing required environment variables. Ensure that you've run `source ./.incremental_assembly_secrets.sh`.")


@task(log_stdout=True)
def fetch_assembly_request(assembly_request_id, SOURCE_ES) -> Tuple[str, list]:
    source_es = Elastic(SOURCE_ES)
    # 1. Fetch assembly-request from source-es by id
    print("Fetching assembly-request")
    assembly_request = source_es.term_query("assembly-request", "id", assembly_request_id)
    project_id = assembly_request["project_id"]
    records = assembly_request["records"]
    print(
        f"Found extension with {len(records)} records for project: {assembly_request_id}")
    return (project_id, records)


@task(log_stdout=True)
def process_cdrs(ASSEMBLY_REQUEST_ID, records, DART_HOST, DART_USER, DART_PASS, TARGET_ES):

    target_es = Elastic(TARGET_ES)
    # 3. Process CDR
    doc_ids = []
    for record in records:
        doc_ids.append(record["document_id"])
    doc_ids = list(set(doc_ids))

    epoch = epoch_millis()
    def cdr_transform_wrapper(obj):
        doc = document_transform(obj)
        doc["origin"] = {
            "assembly_request_id": ASSEMBLY_REQUEST_ID,
            "byod_tag": "Analyst uploads",
            "modified_at": epoch
        }
        return doc

    cdrs = get_CDRs(DART_HOST, DART_USER, DART_PASS, doc_ids)
    counter = 0
    es_buffer = []
    for cdr in cdrs:
        es_buffer.append(cdr_transform_wrapper(cdr))
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
    SOURCE_ES,
    TARGET_ES
):
    source_es = Elastic(SOURCE_ES)
    target_es = Elastic(TARGET_ES)
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
    statement_ids = []
    for statement in new_stmts.values():
        statement_ids.append(statement["id"])
        matches_hash = str(statement["matches_hash"])
        statement["evidence"] = new_evidence.get(matches_hash)
        statement["belief"] = beliefs.get(matches_hash)
        result = influence_transform(statement, source_es)
        counter = counter + 1
        es_buffer.append(result)
        if counter % 500 == 0:
            print(f"\tIndexing ... {counter}")
            target_es.bulk_write(project_id, es_buffer, "matches_hash")
            es_buffer = []
    if len(es_buffer) > 0:
        print(f"\tIndexing ... {counter}")
        target_es.bulk_write(project_id, es_buffer, "matches_hash")
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
        target_es.bulk_write(project_id, update_buffer, "matches_hash")

    return statement_ids

@task(log_stdout=True)
def update_curations(host, SOURCE_ES, project_id, statement_ids):
    source_es = Elastic(SOURCE_ES)
    # need to get kb_id from the project index
    project = source_es.term_query("project", "id", project_id)
    kb_id = project["kb_id"]
    curation = CurationRecommendationAPI(host, SOURCE_ES)
    response = curation.delta_ingest(kb_id, statement_ids, project_id)
    task_id = response["task_id"]
    print(f"Curation delta ingest task id: {task_id}")
    print("Updated curation recommendation to ingest new kb")

@task(log_stdout=True)
def mark_completed(project_id):
    print(f"Updated statements for project {project_id}.")


with Flow("incremental assembly", run_config=LocalRun(labels=["non-dask"])) as flow:
    ASSEMBLY_REQUEST_ID = Parameter("ASSEMBLY_REQUEST_ID")

    if ASSEMBLY_REQUEST_ID is None:
        raise ValueError("Missing parameter ASSEMBLY_REQUEST_ID")

    (
        INDRA_HOST,
        SOURCE_ES,
        TARGET_ES,
        DART_HOST,
        DART_USER,
        DART_PASS,
        CURATION_HOST
    ) = read_environment_variables()
    print_inputs(
        SOURCE_ES,
        TARGET_ES,
        INDRA_HOST,
        ASSEMBLY_REQUEST_ID
    )

    (project_id, records) = fetch_assembly_request(ASSEMBLY_REQUEST_ID, SOURCE_ES)
    process_cdrs_completed = process_cdrs(
        ASSEMBLY_REQUEST_ID,
        records,
        DART_HOST,
        DART_USER,
        DART_PASS,
        TARGET_ES
    )

    response = request_reassembly(project_id, records, INDRA_HOST)
    statement_ids = apply_reassembly_to_es(
        response,
        project_id,
        SOURCE_ES,
        TARGET_ES,
        upstream_tasks=[process_cdrs_completed]
    )
    update_curations(CURATION_HOST, SOURCE_ES, project_id, statement_ids)
    mark_completed(project_id)

# ===========================================================

# Set this to False to run the flow locally, or
# set it to True to register the flow with the Prefect server
# to be run later on an agent.
should_register = False


if (should_register):
    flow.register(project_name="project")
else:
    ASSEMBLY_REQUEST_ID = os.environ.get("ASSEMBLY_REQUEST_ID")
    if ASSEMBLY_REQUEST_ID is None:
        raise ValueError("Missing required environment variable ASSEMBLY_REQUEST_ID")

    state = flow.run(
        executor=LocalExecutor(),
        parameters={
            "ASSEMBLY_REQUEST_ID": ASSEMBLY_REQUEST_ID
        }
    )

    assert state.is_successful()
