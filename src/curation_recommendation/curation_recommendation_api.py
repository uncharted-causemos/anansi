import requests

class CurationRecommendationAPI:
    _host = None
    _es_url = None

    def __init__(self, host, ES_url):
        self._host = host
        self._es_url = ES_url

    def delta_ingest(self, kb_id, statement_ids, project_id):
        r = requests.post(self._host + "/recommendation/delta-ingest/" + kb_id, json={
            "es_host": ":".join(self._es_url.split(':')[:-1]),
            "es_port": self._es_url.split(":")[-1],
            "statement_ids": statement_ids,
            "project_id": project_id
        }, timeout=1200)
        return r.json()
