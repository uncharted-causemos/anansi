import requests

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

    def add_project_records(self, reader_records):
        """
        Submits document_id, reader, and storage key tuples to INDRA for incremental assembly
        """
        r = requests.post(self._url + "/assembly/add_project_records", data = reader_records)
        return r.json()
        
