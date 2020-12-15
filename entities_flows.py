from storey import MapClass, Event
from storey.dtypes import _termination_obj
from mlrun import get_object
from typing import Union, List
import json
import spacy

class BaseClass:
    def __init__(self, context, state=None, name=None):
        self.context = context
        self.state = state
        self.name = name
        
class ApplyNLP(BaseClass):
    def __init__(self, spacy_dict="en_core_web_sm", context=None, state=None, name=None):
#         super().__init__()

        self.nlp = spacy.load(spacy_dict)

    def do(self, paragraphs: List[dict]):
        if paragraphs == _termination_obj:
            return _termination_obj
        else:
            tokenized_paragraphs = []
            paragraphs = json.loads(paragraphs)
            for paragraph in [paragraphs]:
                tokenized = {
                        "url": paragraph["url"],
                        "paragraph_id": paragraph["paragraph_id"],
                        "tokens": self.nlp(paragraph["paragraph"]),
                }
                tokenized_paragraphs.append(tokenized)

            return tokenized_paragraphs

class ExtractEntities(BaseClass):
    def __init__(self, context=None, state=None, name=None):
        pass
    
    def do(self, tokens):
        if tokens == _termination_obj:
            return _termination_obj
        else:
            paragraph_entities = []
            for token in tokens:
                entities = token["tokens"].ents
                for entity in entities:
                    paragraph_entities.append(
                        {
                            "url": token["url"],
                            "paragraph_id": token["paragraph_id"],
                            "entity": entity.ents,
                        }
                    )
            return paragraph_entities


class EnrichEntities(BaseClass):
    def __init__(self, context=None, state=None, name=None):
        pass
    
    def do(self, entities):
        if entities == _termination_obj:
            return _termination_obj
        else:
            enriched_entities = []
            for entity in entities:
                enriched_entities.append(
                    {
                        "url": entity["url"],
                        "paragraph_id": entity["paragraph_id"],
                        "entity_text": entity["entity"][0].text,
                        "entity_start_char": entity["entity"][0].start_char,
                        "entity_end_char": entity["entity"][0].end_char,
                        "entity_label": entity["entity"][0].label_,
                    }
                )
            return enriched_entities