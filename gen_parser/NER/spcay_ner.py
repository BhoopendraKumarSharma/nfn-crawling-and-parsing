import spacy


def extract_orgs(text: str):
    """
    Performs named entity recognition from text
    :param text: Text to extract
    """
    # load spacy nlp library
    spacy_nlp = spacy.load('en_core_web_sm')

    # parse text into spacy document
    doc = spacy_nlp(text.strip())

    # create sets to hold words
    # named_entities = set()
    # money_entities = set()
    organization_entities = set()
    # location_entities = set()
    # time_indicator_entities = set()

    for i in doc.ents:
        entry = str(i.lemma_).lower()
        text = text.replace(str(i).lower(), "")
        # Time indicator entities detection
        # if i.label_ in ["TIM", "DATE"]:
        #     time_indicator_entities.add(entry)
        # money value entities detection
        # elif i.label_ in ["MONEY"]:
        #     money_entities.add(entry)
        # organization entities detection
        if i.label_ in ["ORG"]:
            organization_entities.add(entry)
        # Geographical and Geographical entities detection
        # elif i.label_ in ["GPE", "GEO"]:
        #     location_entities.add(entry)
        # extract artifacts, events and natural phenomenon from text
        # elif i.label_ in ["ART", "EVE", "NAT", "PERSON"]:
        #     named_entities.add(entry.title())

    return organization_entities