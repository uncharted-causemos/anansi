from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


# Simple Elastic wrapper, mostly for indexing and useful for looking up geo and cdr documents
class Elastic:
    _host = None
    _port = None

    def __init__(self, host, port, **kwargs):
        self._host = host
        self.port = port
        self.client = Elasticsearch(host, port=port, **kwargs)

    def term_query(self, index, term, value, **kwargs):
        termBody = {}
        termBody[term] = value
        body = {
            "size": 1, 
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": termBody
                        }
                    ]
                }
            }
        }
        
        result = self.client.search(index=index, body=body, **kwargs)
        return self.parse_result(result)

    def refresh(self, index):
        self.client.indices.refresh(index=index)

    def parse_result(self, r):
        if r == None or r["hits"]["total"]["value"] == 0:
            return None
        doc = r["hits"]["hits"][0]
        if doc == None:
            return None
        return doc["_source"]
