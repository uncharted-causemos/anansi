from elastic import Elastic
ES_HOST = "http://localhost"
ES_PORT = 9200

client = Elastic(ES_HOST, ES_PORT)
indices = client.list_indices().keys()

for index_name in indices:
    print(f"Removing index {index_name}")
    if index_name.startswith("indra"):
        client.set_readonly(index_name, False)
    client.delete_index(index_name)
