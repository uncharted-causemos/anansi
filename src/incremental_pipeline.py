import logging
import json
import os
from smart_open import open
from elastic import Elastic
from requests.auth import HTTPBasicAuth
from indra import IndraAPI
import requests

FORMAT = "%(asctime)-25s %(levelname)-8s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)

from indra import influence_transform, metadata_transfrom, IndraAPI
from utils import json_file_content


# Environment
INDRA_HOST = os.environ.get("INDRA_HOST") # "http://wm.indra.bio/"

# Resources like geolocation and corpus, generally speaking source/target will be the same
SOURCE_ES_HOST = os.environ.get("SOURCE_ES_HOST")
SOURCE_ES_PORT = os.environ.get("SOURCE_ES_PORT")

# Where to index documents
TARGET_ES_HOST = os.environ.get("TARGET_ES_HOST")
TARGET_ES_PORT = os.environ.get("TARGET_ES_PORT")

# Request
PROJECT_EXTENSION_ID = os.environ.get("PROJECT_EXTENSION_ID")


# Fake payload
FAKE_INDRA_REQUEST = {
  "id": "xyz",
  "project_id": "integration-test-1",
  "records": [
      {
          "document_id": "9a8d151e5f40491d47f4dc9b97d47fc8",
          "identity": "eidos",
          "storage_key": "d87fdf3c-2ca9-47af-89e8-3321cf677e59.jsonld",
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

# 2. Extract relevant fields from project-extension document
projectId = extension["project_id"]
records = extension["records"]
logger.info(f"Found extension with {len(records)} records for project: {projectId}")

# 3. Send request to INDRA for reassembly
logger.info("Sending request to INDRA for reassembly")
indra = IndraAPI(INDRA_HOST)
response = indra.add_project_records(projectId, records)

# 4. Parse INDRA response 

new_stmts = response["new_stmts"]
new_evidence = response["new_evidence"]
new_refinements = response["new_refinements"]
beliefs = response["beliefs"]
print(len(new_stmts))
print(len(new_evidence))
print(len(new_refinements))
print(len(beliefs))

if len(new_stmts) > 0:
  print(list(new_stmts.values())[0])

# 5. Pivot "new_stmts" into array of INDRA statements and join with "beliefs", transform and index new statements to project index
counter = 0
es_buffer = []
for statement in new_stmts.values():
  # FIXME: we're not currently handling updates to existing statements, only adding new ones
  # Need to confirm what needs to be done re: refinements/enhancements of existing statements
  result = influence_transform(statement, source_es)
  counter = counter + 1
  es_buffer.append(result)
  if counter % 500 == 0:
      logger.info(f"\tIndexing ... {counter}")
      target_es.bulk_write(projectId, es_buffer)
      es_buffer = []

if len(es_buffer) > 0:
    logger.info(f"\tIndexing ... {counter}")
    target_es.bulk_write(projectId, es_buffer)
    es_buffer = []

# 6. Mark as completed ??
logger.info(f"Updated statements for project {projectId}.")



