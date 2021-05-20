import logging
import json
import os
from smart_open import open
from elastic import Elastic
from requests.auth import HTTPBasicAuth
from dart import document_transform, get_CDRs
import requests

FORMAT = "%(asctime)-25s %(levelname)-8s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)

from indra import influence_transform, metadata_transfrom, evidence_transform, get_wm, IndraAPI
from utils import json_file_content


# Environment
INDRA_HOST = os.environ.get("INDRA_HOST") # "http://wm.indra.bio/"

# Resources like geolocation and corpus, generally speaking source/target will be the same
SOURCE_ES_HOST = os.environ.get("SOURCE_ES_HOST")
SOURCE_ES_PORT = os.environ.get("SOURCE_ES_PORT")

# Where to index documents
TARGET_ES_HOST = os.environ.get("TARGET_ES_HOST")
TARGET_ES_PORT = os.environ.get("TARGET_ES_PORT")

DART_HOST = os.environ.get("DART_HOST");
DART_USER = os.environ.get("DART_USER");
DART_PASS= os.environ.get("DART_PASS");

# Request
PROJECT_EXTENSION_ID = os.environ.get("PROJECT_EXTENSION_ID")


# Fake payload
FAKE_INDRA_REQUEST = {
  "id": "xyz",
  "project_id": "project-fd729e8d-6686-4761-ab7d-2eb906044a65",
  "records": [
      {
          "document_id": "570bc75dba3cfb995445932c57329775",
          "identity": "eidos",
          "storage_key": "a5ff1d64-1371-4edd-ad3e-47d8c899e6bc.jsonld",
          "version": "1.1.0"
      }
  ]
}

# Vars
source_es = Elastic(SOURCE_ES_HOST, SOURCE_ES_PORT)
target_es = Elastic(TARGET_ES_HOST, TARGET_ES_PORT)


# 0. Print out input constants
logger.info(f"Source Elastic: {SOURCE_ES_HOST}:{SOURCE_ES_PORT}")
logger.info(f"Target Elastic: {TARGET_ES_HOST}:{TARGET_ES_PORT}")
logger.info(f"INDRA: {INDRA_HOST}")
logger.info(f"PROJECT_EXTENSION_ID: {PROJECT_EXTENSION_ID}")

# 1. Fetch project-extension from source-es by id
logger.info("Fetching project-extension")

extension = source_es.term_query("project-extension", "_id", PROJECT_EXTENSION_ID)
# FIXME - temporary test
extension = FAKE_INDRA_REQUEST

# 2. Extract relevant fields from project-extension document
project_id = extension["project_id"]
records = extension["records"]
logger.info(f"Found extension with {len(records)} records for project: {project_id}")


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
        logger.info(f"\tIndexing ... {counter}")
        target_es.bulk_write('corpus', es_buffer)

if len(es_buffer) > 0:
    target_es.bulk_write('corpus', es_buffer)


# 4. Send request to INDRA for reassembly
logger.info("Sending project-extension request to INDRA for reassembly")
indra = IndraAPI(INDRA_HOST)
response = indra.add_project_records(project_id, records)

# 5. Parse INDRA response
new_stmts = response["new_stmts"]
new_evidence = response["new_evidence"]
new_refinements = response["new_refinements"]
beliefs = response["beliefs"]
logger.info("=" * 50)
logger.info(f"{len(new_stmts)} new statements.")
logger.info(f"{len(new_evidence)} new pieces of evidence.")
logger.info(f"{len(new_refinements)} new refinements.")
logger.info(f"{len(beliefs)} belief scores.")
logger.info("=" * 50)

# new_stmts = {} # Testing

# 6. Pivot "new_stmts" into array of INDRA statements and join with "beliefs", transform and index new statements to project index
logger.info("")
logger.info("Processing new statements")
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
      logger.info(f"\tIndexing ... {counter}")
      target_es.bulk_write(project_id, es_buffer)
      es_buffer = []

if len(es_buffer) > 0:
    logger.info(f"\tIndexing ... {counter}")
    target_es.bulk_write(project_id, es_buffer)
    es_buffer = []

# 7 Merge new evidence 
# Not very efficient, should do batch queries and partial fetches + update
logger.info("")
logger.info("Processing updated statements")
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
    logger.info(f"\tIndexing ... {len(update_buffer)}")
    target_es.bulk_write(project_id, update_buffer)


# 8. Mark as completed ??
logger.info(f"Updated statements for project {project_id}.")
