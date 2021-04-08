import requests
from utils import get_event_time

"""
Transforms DART CDR to Causemos format.
- Discard noisy analyics
- Pre-aggregate the analytics we actually want
- Normalize fields where applicable
"""
def document_transform(doc):
    extracted_metadata = doc["extracted_metadata"]
    publication_date = get_event_time(extracted_metadata["CreationDate"])

    ner = get_NER(doc)

    # print(publication_date)
    return {
        "doc_id": doc["document_id"],
        "file_name": doc["source_uri"],
        "file_type": doc["content_type"],
        "doc_title": extracted_metadata.get("Title", ""),
        "author": extracted_metadata.get("Author", ""),
        "publisher_name": extracted_metadata.get("Publisher", ""),
        "publication_date": publication_date,
        "extracted_text":   doc["extracted_text"],
        "collection_type":  doc["capture_source"],
        "ner_analytics": ner,
        "analysis": []
    }


def get_NER(doc):
    annotations = doc["annotations"]
    contents = []
    for annotation in annotations:
        if annotation["type"] == "tags":
            contents = contents + annotation["content"]

    word_counter = {}
    word_counter["org"] = {}
    word_counter["loc"] = {}

    for item in contents:
        if "value" in item == False or "tag" in item == False:
            continue

        tag = item["tag"].lower()
        if tag != "loc" and tag != "org":
            continue
        
        # FIXME: need to scrub text
        value = item["value"]

        if value in word_counter[tag]:
            word_counter[tag][value] = world_counter[tag][value] + 1
        else:
            word_counter[tag][value] = 1

    sorted_org = sorted(word_counter["org"].items(), key = lambda x: x[1])
    sorted_loc = sorted(word_counter["loc"].items(), key = lambda x: x[1])

    top_org = list(map(lambda x: x[0], sorted_org))[:5]
    top_loc = list(map(lambda x: x[0], sorted_loc))[:5]
    return {
        "loc": top_loc,
        "org": top_org
    }
