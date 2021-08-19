import requests
import json

class IndraAPI:
    _url = None

    def __init__(self, url):
        self._url = url

    def health(self):
        """
        Basic health check
        """
        r = requests.get(self._url + "/health")
        return r.json()

    def add_project_records(self, project_id, reader_records):
        payload = {
            "project_id": project_id,
            "records": reader_records
        }
        """
        Submits document_id, reader, and storage key tuples to INDRA for incremental assembly
        """
        print("*************************")
        print(json.dumps(payload))
        print("*************************")
        r = requests.post(self._url + "/assembly/add_project_records", json = payload, timeout=30000)
        return r.json()
        
