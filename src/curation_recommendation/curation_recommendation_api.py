import requests

class CurationRecommendationAPI:
    _host = None

    def __init__(self, host):
        self._host = host

    def ingest(self, kb_id):
        r = requests.post(self._host + "/delta-ingest/" + kb_id, timeout=1200)
        return r.json()

