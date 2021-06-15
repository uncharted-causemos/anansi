from elastic import Elastic
ES= "http://localhost:9200"

client = Elastic(ES)
indices = client.list_indices().keys()

for index_name in indices:
    if index_name.startswith("demo-empty"):
        continue

    print(f"Removing index {index_name}")

    if index_name.startswith("indra"):
        client.set_readonly(index_name, False)
    client.delete_index(index_name)
