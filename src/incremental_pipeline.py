import logging
import json
import os
from smart_open import open
from elastic import Elastic
from indra import influence_transform, metadata_transfrom, IndraAPI
from dart import document_transform
from utils import json_file_content
from requests.auth import HTTPBasicAuth

FORMAT = "%(asctime)-25s %(levelname)-8s %(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger("X")
logger.setLevel(20)

# Environment
INDRA_HOST = os.environ.get("INDRA_HOST") # "http://wm.indra.bio/"
DART_HOST = os.environ.get("DART_HOST")
DART_USER = os.environ.get("DART_USER")
DART_PASS = os.environ.get("DART_PASS")

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


# 1. Fetch project-extension from source-es by id

# 2. Retreive, transform, and index document in project-extension doc

# 3. Send request to INDRA for reassembly

# 4. Parse INDRA response 

# 5. Pivot "new_stmts" into array of INDRA statements and join with "beliefs", transform and index new statements to project index

# 6. Mark as completed ??



