import requests
from utils import get_event_time

def document_transform(doc):
    """
    Transforms DART CDR to Causemos format.
    """
    extracted_metadata = doc["extracted_metadata"]
    publication_date = get_event_time(extracted_metadata["CreationDate"])

    ner = get_NER(doc)
    analysis = get_analysis(doc)

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
        "analysis": analysis
    }


def get_analysis(doc):
    """
    Extract stance and sentiment
    """
    stance = ""
    subjectivity = ""
    subjectivity_score = 0
    sentiment = ""
    sentiment_score = 0

    annotations = doc["annotations"]
    for annotation in annotations:
        if annotation["type"] != "tags":
            continue

        if annotation["label"] == "Qntfy Fake News":
            stance = annotation["content"][0]["value"]
        elif annotation["label"] == "Qntfy Sentiment/Subjectivity":
            content = annotation["content"]
            for item in content:
                v = item["value"]
                if v == "objective" or v == "subjective":
                    subjectivity = v
                    subjectivity_score = item["confidence"]
                else:
                    sentiment = v
                    sentiment_score = item["confidence"]
    return {
        "stance": stance,
        "sentiment": sentiment,
        "sentiment_score": sentiment_score,
        "subjectivity": subjectivity,
        "subjectivity_score": subjectivity_score
    }


def get_NER(doc):
    """ 
    Extract location and organization entities from analytics. This is pretty noisy so just
    return to top 5 workd-tokens based on world count.
    """
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
