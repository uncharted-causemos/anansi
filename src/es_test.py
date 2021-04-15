from elastic import Elastic

# Environment
ES_HOST = "http://localhost"
ES_PORT = 9200


# Test ES
es = Elastic(ES_HOST, ES_PORT)

# es.delete_index("dc-test")
# es.create_index("dc-test")

doc = {
    "id": "123",
    "value": "def-delicious"
}
es.bulk_write("dc-test", [doc])
