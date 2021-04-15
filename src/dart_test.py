from smart_open import open
import json
from dart import document_transform

DART_JSONL = "file:///Users/dchang/workspace/worldmodelers/anansi/src/sample-corpus.jsonl"
# test2 = "http://10.64.16.209:4005/twosix-indra/sample2.json" 
with open(DART_JSONL, 'r') as F:
    for line in F:
        cdr = json.loads(line)
        doc = document_transform(cdr)
        print(doc)
