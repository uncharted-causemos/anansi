#!/usr/bin/env bash

ES=http://localhost:9200
# ES=http://10.65.18.69:9200

echo "Remove kb entry"
curl -XDELETE -H "Content-type: application/json" $ES/knowledge-base/_doc/indra
echo ""
echo ""

echo "Add kb entry"
curl -XPUT -H "Content-type: application/json" $ES/knowledge-base/_doc/indra -d'
{
  "id": "indra",
  "ontology": "https://raw.githubusercontent.com/WorldModelers/Ontologies/master/CompositionalOntology_v2.1_metadata.yml",
  "corpus_id": null,
  "created_at": 0,
  "name": "demo-empty",
  "corpus_parameter": {
    "display_name": "demo-empty"
  }
}
'
echo ""
echo ""
