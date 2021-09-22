import logging
import json
import os
import uuid
from smart_open import open
from elastic import Elastic
from indra import influence_transform, metadata_transfrom, IndraAPI
from dart import document_transform
from utils import json_file_content, epoch_millis

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

# SOURCE_ES_HOST = "http://10.64.18.99"
SOURCE_ES = os.environ.get("SOURCE_ES")

TARGET_ES = os.environ.get("TARGET_ES")

DART_DATA = os.environ.get("DART_DATA")
INDRA_DATASET = os.environ.get("INDRA_DATASET")
INDRA_STATEMENTS = INDRA_DATASET + "/statements.json"
INDRA_METADATA = INDRA_DATASET + "/metadata.json"

ONTOLOGY_URL = "https://raw.githubusercontent.com/WorldModelers/Ontologies/master/CompositionalOntology_metadata.yml"

indra_dataset_id = "indra-" + str(uuid.uuid4());

# Vars
source_es = Elastic(SOURCE_ES)
target_es = Elastic(TARGET_ES, timeout=300)

def JSONL_ETL_wrapper(filename, transform_fn, index_name, key = "id"):
    counter = 0
    es_buffer = []
    with open(filename, 'r') as F:
        for line in F:
            counter = counter +1
            obj = json.loads(line)
            doc = transform_fn(obj)
            es_buffer.append(doc)
            if counter % 500 == 0:
                logger.info(f"\tIndexing ... {counter}")
                target_es.bulk_write(index_name, es_buffer, key)
                es_buffer = []

    if len(es_buffer) > 0:
        logger.info(f"\tIndexing ... {counter}")
        target_es.bulk_write(index_name, es_buffer, key)
        es_buffer = []


# 1. Print out info
logger.info(f"Creating new INDRA dataset: {indra_dataset_id}")
logger.info(f"Source Elastic: {SOURCE_ES}")
logger.info(f"Target Elastic: {TARGET_ES}")
logger.info(f"INDRA: {INDRA_DATASET}")
logger.info(f"DART: {DART_DATA}")

# 2. Load CDRs
epoch = epoch_millis()
def cdr_transform_wrapper(obj):
    doc = document_transform(obj)
    doc["origin"] = {
        "assembly_request_id": "init",
        "byod_tag": "Default",
        "modified_at": epoch
    }
    return doc

logger.info("Indexing CDRs")
JSONL_ETL_wrapper(DART_DATA, cdr_transform_wrapper, "corpus")
target_es.refresh("corpus")


# 3. Load INDRA statements
def indra_transform(obj):
    return influence_transform(obj, source_es)

logger.info("Cloning 'indra' index")
try:
    target_es.clone("indra", indra_dataset_id)
    target_es.refresh(indra_dataset_id)
    # target_es.set_readonly(indra_dataset_id, False)
    # target_es.refresh(indra_dataset_id)
    logger.info(f"Created index {indra_dataset_id}")
except Exception as e:
    logger.error(f"Failed to create index {indra_dataset_id}")

logger.info("Indexing INDRA statements")
JSONL_ETL_wrapper(INDRA_STATEMENTS, indra_transform, indra_dataset_id, "matches_hash")

logger.info(f"Done");
index_data = target_es.cat_index(indra_dataset_id)
logger.info(f"\t{index_data}");

target_es.set_readonly(indra_dataset_id, True)


# 4. Create knowledge base entry
logger.info("Creating knowledge base entry")
metadata = json_file_content(INDRA_METADATA);
kb_doc = metadata_transfrom(metadata, indra_dataset_id, ONTOLOGY_URL)
target_es.bulk_write("knowledge-base", [kb_doc])

logger.info("All done!!!")
