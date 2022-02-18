import time
import statistics
from utils import get_event_time, epoch_millis


def metadata_transfrom(metadata, uuid, ontology_url):
    """
    Transforms INDRA metadata into a Causemos Knowledgebase entry record
    """
    corpus_id = metadata["corpus_id"]

    return {
        "corpus_parameter": metadata,
        "ontology": ontology_url,
        "corpus_id": corpus_id,
        "name": corpus_id,
        "id":   uuid,
        "tenant_id": metadata.get("tenant", None),
        "created_at": epoch_millis()
    }


def influence_transform(statement, es):
    """
    Entry for Influece statement transform
    """
    evidence = get_evidence(statement, es)
    subj = get_event(statement, "subj", es)
    obj = get_event(statement, "obj", es)
    wm = get_wm(evidence, subj, obj)

    return {
        "id": statement["id"],
        "matches_hash": str(statement["matches_hash"]),
        "belief": statement["belief"],
        "evidence": evidence,
        "modified_at": round(time.time() * 1000),
        "obj": obj,
        "subj": subj,
        "wm": wm
    }


def get_wm(evidence, subj, obj):
    """
    Precomputed fields
    """
    wm = {}
    readers_count = {
        "hume": 0,
        "eidos": 0,
        "cwms": 0,
        "sofia": 0
    }
    readers = []
    contradictions = 0
    hedgings = 0
    for ev in evidence:
        evidence_context = ev["evidence_context"]
        reader = evidence_context["source_api"]
        readers_count[reader] = readers_count[reader] + 1
        readers.append(reader)
        if len(evidence_context["hedging_words"]) > 0:
            hedgings = hedgings + 1
        if len(evidence_context["contradiction_words"]) > 0:
            contradictions = contradictions + 1

    readers = list(set(readers))

    num_evidence = len(evidence)

    # Set hedging categories:
    # 0: no evidence with hedging
    # 1: some evidence with hedging
    # 2: all evidence has hedging
    hedging_category = 0
    if hedgings > 0 and hedgings < num_evidence: 
        hedging_category = 1
    elif hedgings == num_evidence: 
        hedging_category = 2

    # Set contradictions categories:
    # 0: no evidence with negation flag
    # 1: some evidence with negation flag
    # 2: all evidence has negation flag
    contradiction_category = 0
    if contradictions > 0 and contradictions < num_evidence: 
        contradiction_category = 1
    elif contradictions == num_evidence: 
        contradiction_category = 2

    loop = False
    if subj["concept"] == obj["concept"]:
        loop = True
    edge = subj["concept"] + "///" + obj["concept"]
    statement_polarity = subj["polarity"] * obj["polarity"]
    min_grounding_score = min(subj["concept_score"], obj["concept_score"])

    wm["num_evidence"] = num_evidence
    wm["num_contradictions"] = contradictions
    wm["contradiction_category"] = contradiction_category
    wm["hedging_category"] = hedging_category
    wm["readers_evidence_count"] = readers_count
    wm["is_selfloop"] = loop
    wm["statement_polarity"] = statement_polarity
    wm["edge"] = edge
    wm["state"] = 1
    wm["edited"] = 0
    wm["readers"] = readers
    wm["min_grounding_score"] = min_grounding_score
    return wm


# Hack for a quick and dirty cache
document_context_cache = {}

def evidence_transform(ev, es):
    """
    Transform and enrich a single evidence
    """
    evidence_context = {}
    document_context = {}
    hedgings = []
    negated_texts = []

    # Parsing evidence
    if "epistemics" in ev and "hedgings" in ev["epistemics"]: 
        hedgings = ev["epistemics"]["hedgings"]

    if "negated_texts" in ev["annotations"]:
        negated_texts = ev["annotations"]["negated_texts"]

    evidence_context["source_api"] = ev["source_api"]
    evidence_context["text"] = ev["text"]
    evidence_context["agents_text"] = ev["annotations"]["agents"]["raw_text"]
    evidence_context["source_hash"] = ev["source_hash"]
    evidence_context["hedging_words"] = hedgings
    evidence_context["contradiction_words"] = negated_texts
    if "subj_polarity" in ev["annotations"]:
        evidence_context["subj_polarity"] = ev["annotations"]["subj_polarity"]
    if "obj_polarity" in ev["annotations"]:
        evidence_context["obj_polarity"] = ev["annotations"]["obj_polarity"]

    if "subj_adjectives" in ev["annotations"]:
        evidence_context["subj_adjectives"] = ev["annotations"]["subj_adjectives"]
    else:
        evidence_context["subj_adjectives"] = []
    if "obj_adjectives" in ev["annotations"]:
        evidence_context["obj_adjectives"] = ev["annotations"]["obj_adjectives"]
    else:
        evidence_context["obj_adjectives"] = []

    # Parsing document
    dart = ev["text_refs"]["DART"]
    if dart in document_context_cache:
        document_context = document_context_cache[dart]
    else:
        cdr = es.term_query("corpus", "id", dart)
        if cdr != None:
            document_context["file_type"] = cdr["file_type"]
            document_context["author"] = cdr["author"]
            document_context["document_source"] = cdr["collection_type"] # ????
            document_context["publisher_name"] = cdr["publisher_name"]
            document_context["title"] = cdr["doc_title"]
            document_context["genre"] = cdr["genre"]
            document_context["ner_analytics"] = cdr["ner_analytics"]
            document_context["analysis"] = cdr["analysis"]
            document_context["label"] = cdr["label"]

            # BYOD tagging
            if "origin" in cdr:
                document_context["origin"] = cdr["origin"]

            document_context["publication_date"] = {}
            if "publication_date" in cdr and cdr["publication_date"] is not None:
                document_context["publication_date"]["date"] = int(cdr["publication_date"]["date"])
                document_context["publication_date"]["year"] = cdr["publication_date"]["year"]
                document_context["publication_date"]["month"] = cdr["publication_date"]["month"]
                document_context["publication_date"]["day"] = cdr["publication_date"]["day"]
        document_context["doc_id"] = dart
        document_context_cache[dart] = document_context

    return { "document_context": document_context, "evidence_context": evidence_context }



def get_evidence(statement, es):
    """
    Parse evidnece into evidence_context and document_context
    """
    evidence = statement["evidence"]
    result = []

    for ev in evidence:
        result.append(evidence_transform(ev, es));
    return result


def get_event(statement, t, es):
    """
    Extract subject or object event based on t
    """
    event = statement[t]

    time_context = {}
    geo_context = {}
    adjectives = []

    if "adjectives" in event["delta"]:
        adjectives = event["delta"]["adjectives"]

    if "context" in event:
        if "time" in event["context"] and event["context"]["time"] != None:
            time_context["start"] = get_event_time(event["context"]["time"]["start"])
            time_context["end"] = get_event_time(event["context"]["time"]["end"])

        if "geo_location" in event["context"] and event["context"]["geo_location"] != None:
            geo = None
            if "GEOID" in event["context"]["geo_location"]["db_refs"]:
                geo_id = event["context"]["geo_location"]["db_refs"]["GEOID"]
                geo = es.term_query("geo", "geo_id", geo_id)
            if geo != None:
                geo_context["name"] = geo["name"]
                geo_context["location"] = {
                    "lat": geo["lat"],
                    "lon": geo["lon"]
                }

    candidates = get_candidates(event)
    top = candidates[0]

    return {
        "factor": event["concept"]["db_refs"]["TEXT"],
        "polarity": event["delta"]["polarity"],
        "time_context": time_context,
        "adjectives": adjectives,
        "concept": top["name"],
        "concept_score": top["score"],
        "theme": top["theme"],
        "theme_property": top["theme_property"],
        "process": top["process"],
        "process_property": top["process_property"],
        "candidates": candidates,
        "geo_context": geo_context
    }



def get_candidates(event): 
    """
    Parse out candidates from WM and WM_FLAT structures
    """
    candidates = []
    if "WM" in event["concept"]["db_refs"]:
        flat_list = []
        orig_list = event["concept"]["db_refs"]["WM"] 

        # In case the flattened list is not avaialble
        if "WM_FLAT" in event["concept"]["db_refs"]:
            flat_list = event["concept"]["db_refs"]["WM_FLAT"]
        else:
            for idx, candidate in enumerate(orig_list):
                theme_grounding = candidate[0][0]
                other_groundings = [entry[0].split('/')[-1] for entry in candidate[1:] if entry]
                flat_grounding = '_'.join([theme_grounding] + other_groundings)
                score = statistics.mean([entry[1] for entry in candidate if entry is not None])
                flat_list.append({
                    "grounding": flat_grounding,
                    "score": score,
                    "name": "dummy" # name not used
                })


        for idx, _ in enumerate(flat_list):
            name = flat_list[idx]["grounding"]

            # HACK: May need to remove trailing spaces that are found for SOFIA/CWMS reader
            if name[-1] == "/":
                name = name[:-1]

            score = flat_list[idx]["score"]
            theme = ""
            theme_property = ""
            process = ""
            process_property = ""

            if orig_list[idx][0] != None:
                theme = orig_list[idx][0][0]

            if orig_list[idx][1] != None:
                theme_property = orig_list[idx][1][0]

            if orig_list[idx][2] != None:
                process = orig_list[idx][2][0]

            if orig_list[idx][3] != None:
                process_property = orig_list[idx][3][0]

            candidates.append({
                "name": name,
                "score": score,
                "theme": theme,
                "theme_property": theme_property,
                "process": process,
                "process_property": process_property
            })
    else:
        candidates.append({
            "name": "UNKNOWN",
            "score": 0,
            "theme": "",
            "theme_property": "",
            "process": "",
            "process_property": ""
        })
    return candidates
