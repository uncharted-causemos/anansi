import requests

class CurationRecommendationAPI:
    _host = None
    _es_url = None

    def __init__(self, host, ES_url):
        self._host = host
        self._es_url = ES_url

    def ingest(self, kb_id, statement_ids):
        r = requests.post(self._host + "/delta-ingest/" + kb_id, json={
            "es_host": self._es_url.split(":")[-2],
            "es_port": self._es_url.split(":")[-1],
            "statement_ids": statement_ids
        }, timeout=1200)
        return r.json()

