import requests
import logging
from requests.auth import HTTPBasicAuth
from utils import get_event_time

logger = logging.getLogger(__name__)

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
        "id": doc["document_id"],
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
    stance = "" # FIXME: Qntify no longer provide stance, to be removed
    subjectivity = ""
    subjectivity_score = 0
    sentiment = ""
    sentiment_score = 0

    annotations = doc["annotations"]
    for annotation in annotations:
        if annotation["type"] != "facets":
            continue

        if annotation["label"] == "qntfy-sentiment-annotator":
            content = annotation["content"]
            for item in content:
                v = item["value"]
                score = item["score"]
                if v == "sentiment":
                    if score > 0.55:
                        sentiment = "positive"
                    elif score < 0.45:
                        sentiment = "negative"
                    else:
                        sentiment = "neutral"
                    sentiment_score = score
                    
                if v == "subjectivity":
                    if score > 0.5:
                        subjectivity = "objective"
                    else:
                        subjectivity = "subjective"
                    subjectivity_score = score
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

    location_black_list = ["east", "west", "north", "south", "earth", "central"]

    for item in contents:
        if "value" in item == False or "tag" in item == False:
            continue

        tag = item["tag"].lower()
        if tag != "loc" and tag != "org":
            continue

        # FIXME: need to scrub text
        value = item.get("value", "")

        if tag == "loc" and value.lower() in location_black_list:
            continue

        if value in word_counter[tag]:
            word_counter[tag][value] = word_counter[tag][value] + 1
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


def get_CDRs(api_base, username, password, doc_ids):
    """
    Fetch CDRs as JSONs from DART service
    """
    cdrs = []
    try:
        for doc_id in doc_ids:
            url = api_base + "/cdrs/" + doc_id
            logger.info(f"Processing {url}")
            response = requests.get(url, auth=HTTPBasicAuth(username, password), timeout=10)

            if response.status_code > 200:
                logger.info(f"Cound not retrieve CDR for {doc_id}")
                continue
            cdrs.append(response.json())
        return cdrs
    except:
        logger.error("Failed to retrieve CDRs, return empty list")
        return []
