from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

es_bulk_config = {
    "chunk_size": 10000,
    "max_chunk_bytes": 5000000,
    "max_retries": 8,
    "initial_backoff": 3,
    "max_backoff": 300,
    "raise_on_error": True,
    "timeout": "60s"
}

def _format_for_es(index, data):
    if not isinstance(data, list):
        data = [data]

    for datum in data:
        yield {
            "_source": datum,
            "_index": index,
            "_id": datum["id"]
        }

# Simple Elastic wrapper, mostly for indexing and useful for looking up geo and cdr documents
class Elastic:
    _esURL = None

    def __init__(self, esURL, **kwargs):
        self._esURL = esURL
        self.client = Elasticsearch([esURL], **kwargs)

    def term_query(self, index, term, value, **kwargs):
        """
        Find by id query
        """
        termBody = {}
        termBody[term] = value
        body = {
            "size": 1, 
            "_source": {
                "excludes": ["extracted_text"]
            },
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

    def bulk_write(self, index, data):
        """
        Bulk write to ES
        """
        ok, response = bulk(self.client, _format_for_es(index, data), **es_bulk_config)

        if not ok:
            body = response[0]["index"]
            status = body["status"]

            # 400 range status signifies an error has ocurred
            if status in range(400, 500):
                error = body["error"]
                raise Exception("{} - {}".format(error["type"], error["reason"]))
        return response

    def list_indices(self):
        response = self.client.indices.get("*")
        return response

    def create_index(self, index, mappings={}):
        """
        Create an index in ES w/ or w/o a body
        """

        settings = {
            "index.number_of_shards": 1,
            "index.number_of_replicas": 0
        }

        response = self.client.indices.create(
            index=index,
            body={"mappings": mappings, "settings": settings},
            ignore=400
        )
        return response

    def set_readonly(self, index, v):
        body = {
            "index": {
                "blocks.read_only": v
            }
        }
        response = self.client.indices.put_settings(body, index)
        return response

    def delete_index(self, index):
        """
        Delete an index in ES
        """
        response = {}
        if self.client.indices.exists(index):
            response = self.client.indices.delete(index=index)
        return response

    def refresh(self, index):
        self.client.indices.refresh(index=index)

    def parse_result(self, r):
        if r == None or r["hits"]["total"]["value"] == 0:
            return None
        doc = r["hits"]["hits"][0]
        if doc == None:
            return None
        return doc["_source"]
