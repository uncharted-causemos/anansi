from utils import get_event_time

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
        "belief": statement["belief"],
        "evidence": evidence,
        "modified_at": 0,
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


def get_evidence(statement, es):
    """
    Parse evidnece into evidence_context and document_context
    """
    evidence = statement["evidence"]
    result = []

    for ev in evidence:
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
        evidence_context["subj_adjectives"] = ev["annotations"]["subj_adjectives"]
        evidence_context["subj_polarity"] = ev["annotations"]["subj_polarity"]
        evidence_context["obj_adjectives"] = ev["annotations"]["obj_adjectives"]
        evidence_context["obj_polarity"] = ev["annotations"]["obj_polarity"]

        # Parsing document
        dart = ev["text_refs"]["DART"]
        cdr = es.term_query("corpus", "doc_id", dart)
        if cdr != None:
            document_context["file_type"] = cdr["file_type"]
            document_context["author"] = cdr["author"]
            document_context["document_source"] = cdr["collection_type"] # ????
            document_context["publisher_name"] = cdr["publisher_name"]
            document_context["title"] = cdr["doc_title"]
            document_context["ner_analytics"] = cdr["ner_analytics"]
            document_context["analysis"] = cdr["analysis"]
            document_context["publication_date"] = cdr["publication_date"]
        document_context["doc_id"] = dart

        result.append({
            "evidence_context": evidence_context,
            "document_context": document_context
        })
        return result


def get_event(statement, t, es):
    """
    Extract subject or object event based on t
    """
    event = statement[t]

    time_context = {}
    geo_context = {}
    adjectives = []

    if "time" in event["context"]:
        time_context["start"] = get_event_time(event["context"]["time"]["start"])
        time_context["end"] = get_event_time(event["context"]["time"]["end"])

    if "adjectives" in event["delta"]:
        adjectives = event["delta"]["adjectives"]

    if event["context"]["geo_location"] != None:
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
        "factor": event["concept"]["name"],
        "polarity": event["delta"]["polarity"],
        "time_context": time_context,
        "adjective": adjectives,
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
        flat_list = event["concept"]["db_refs"]["WM_FLAT"]
        orig_list = event["concept"]["db_refs"]["WM"] 

        for idx, _ in enumerate(flat_list):
            name = flat_list[idx]["grounding"]
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
