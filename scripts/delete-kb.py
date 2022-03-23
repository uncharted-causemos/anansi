## Removes a knowledge-base and associated artifacts
import sys
from elasticsearch import Elasticsearch
from elasticsearch.client import CatClient


KB_ID="yyy"
SOURCE_ES="yyy"
SOURCE_USERNAME="yyy"
SOURCE_PASSWORD="yyy"


def set_readonly(client, index, v):
    body = {
        "index": {
            "blocks.read_only": v,
            "blocks.write": v
        }
    }
    response = client.indices.put_settings(body, index)
    return response

def delete_index(client, index):
    response = {}
    if client.indices.exists(index):
        response = client.indices.delete(index=index)
    return response

def delete_document(client, index, doc_id):
    response = {}
    if client.indices.exists(index):
        response = client.delete(index=index, id=doc_id)
    return response


if KB_ID is None or KB_ID == "":
    print("Invalid input")
    sys.exit()

clent = None
kb_index = KB_ID
curation_factor_index = "curation-factor-" + kb_index
curation_concept_index = "curation-concept-" + kb_index
curation_statement_index = "curation-statement-" + kb_index

if SOURCE_USERNAME == None or SOURCE_PASSWORD == None:
    client = Elasticsearch([SOURCE_ES])
else:
    client = Elasticsearch(SOURCE_ES, http_auth=(SOURCE_USERNAME, SOURCE_PASSWORD))

print(f"\nCleaning {KB_ID}")
try: 
    set_readonly(client, kb_index, False)
except:
    pass
print("\nCleaning curation indices")
delete_index(client, curation_factor_index)
delete_index(client, curation_statement_index)
delete_index(client, curation_concept_index)
print("\nCleaning KB index")
delete_index(client, kb_index)
print("\nCleaning KB entry")
delete_document(client, "knowledge-base", KB_ID)
