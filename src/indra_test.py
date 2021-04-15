from smart_open import open
import json
from elastic import Elastic
from indra import influence_transform, IndraAPI

# vars
ES_HOST = "http://10.64.18.99"
ES_PORT = 9200
INDRA_HOST = "http://wm.indra.bio/"
INDRA_JSONL = "file:///Users/dchang/workspace/worldmodelers/anansi/src/sample-indra.jsonl"

es = Elastic(ES_HOST, ES_PORT)

# Health check
indra_api = IndraAPI(INDRA_HOST)
health_result = indra_api.health()
print(health_result)


# Parsing test
with open(INDRA_JSONL, 'r') as F:
    for line in F:
        statement = json.loads(line)
        doc = influence_transform(statement, es)
        print(doc)
