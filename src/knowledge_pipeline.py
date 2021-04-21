import logging
import json
import uuid
from smart_open import open
from elastic import Elastic
from indra import influence_transform, metadata_transfrom, IndraAPI
from dart import document_transform

FORMAT = "%(asctime)-25s %(levelname)-8s %(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger("X")
logger.setLevel(20)


"""
Knowledge pipeline for processing INDRA dataset and DART CDRs as file resources.
It assumes the necessary dependencies (CDRs, Geonames) are already setup and can be used.

See ./scripts/download_indra_s3.py for grabbing raw INDRA data
See ./scripts/build_dart.sh for creating a usable DART CDR json from raw DART outputs.
"""

# Environment
INDRA_HOST = "http://wm.indra.bio/"

SOURCE_ES_HOST = "http://10.64.18.99"
SOURCE_ES_PORT = 9200

TARGET_ES_HOST = "http://localhost"
TARGET_ES_PORT = 9200

DART_DATA = "file:///Users/dchang/workspace/worldmodelers/anansi/src/sample-corpus.jsonl"
INDRA_DATASET = "file:///Users/dchang/workspace/worldmodelers/anansi/scripts/phase3_eidos_v3"
INDRA_STATEMETNS = INDRA_DATASET + "/statements.json"
INDRA_METADATA = INDRA_DATASET + "/metadata.json"

ONTOLOGY_URL = "https://raw.githubusercontent.com/WorldModelers/Ontologies/master/CompositionalOntology_v2.1_metadata.yml"

indra_dataset_id = "indra-" + str(uuid.uuid4());

# Vars
source_es = Elastic(SOURCE_ES_HOST, SOURCE_ES_PORT)
target_es = Elastic(TARGET_ES_HOST, TARGET_ES_PORT)

# 0. Print out info
logger.info(f"Creating new INDRA dataset: {indra_dataset_id}")
logger.info(f"Source Elastic: {SOURCE_ES_HOST}:{SOURCE_ES_PORT}")
logger.info(f"Target Elastic: {TARGET_ES_HOST}:{TARGET_ES_PORT}")
logger.info(f"INDRA: {INDRA_DATASET}")
logger.info(f"DART: {DART_DATA}")

# 1. Read dataset metadata
with open(INDRA_METADATA, 'r') as F:
    metadata = F.read()
    metadata = json.loads(metadata)

# 2. Load CDRs
with open(DART_DATA, 'r') as F:
    for line in F:
        cdr = json.loads(line)
        doc = document_transform(cdr)

# 3. Load INDRA statements
# FIXME: Need better way to handle mappings
with open("file:///Users/dchang/workspace/worldmodelers/anansi/src/indra/indra_mapping.json") as F:
    mapping_content = F.read()
    mapping_content = json.loads(mapping_content);
    try:
        target_es.create_index(indra_dataset_id, mapping_content)
        logger.info(f"Created index {indra_dataset_id}")
    except:
        logger.error(f"Failed to create knowledge-base entry {indra_dataset_id}")

counter = 0
indra_buffer = []
with open(INDRA_STATEMETNS, 'r') as F:
    for line in F:
        counter = counter +1
        statement = json.loads(line)
        doc = influence_transform(statement, source_es)
        indra_buffer.append(doc)
        if counter % 500 == 0:
            logger.info(f"Indexing ... {counter}")
            target_es.bulk_write(indra_dataset_id, indra_buffer)
            indra_buffer = []

if len(indra_buffer) > 0:
    logger.info(f"Indexing ... {counter}")
    target_es.bulk_write(indra_dataset_id, indra_buffer)
    indra_buffer = []


# 4. Create knowledge base entry
kb_doc = metadata_transfrom(metadata, indra_dataset_id, ONTOLOGY_URL)
target_es.bulk_write("knowledge-base", [kb_doc])
