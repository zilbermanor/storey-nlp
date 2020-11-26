from storey import MapClass, Event
from storey.dtypes import _termination_obj
from mlrun import get_object
from typing import Union, List
import json
import spacy


class URLDownloader(MapClass):
    def __init__(self, prefix: str = None, secrets: dict = None):

        super().__init__()
        self.secrets = secrets

    def do(self, urls: Union[str, Event, List]):
        url_list = []
        if type(urls) == Event:
            url_list = urls.Body
        elif type(urls) == str:
            url_list = [urls]
        elif urls == _termination_obj:
            return self._do_downstream(_termination_obj)
        else:
            url_list = urls
        docs = []
        for url in url_list:
            try:
                doc = json.loads(get_object(url, self.secrets).decode("utf-8"))
                docs.append({"url": url, "doc": doc})
            except Exception as e:
                print(f"Couldnt get {url}, {e}")
        return docs


def to_paragraphs(docs: Union[List, Event]):
    if docs == _termination_obj or docs is None:
        print("No doc")
        return _termination_obj
    else:
        paragraphs = []
        for doc in docs:
            for i, paragraph in enumerate(doc["doc"]):
                paragraphs.append(
                    [{"url": doc["url"], "paragraph_id": i, "paragraph": paragraph}]
                )
        return paragraphs


class apply_nlp(MapClass):
    def __init__(self, spacy_dict="en_core_web_sm"):
        super().__init__()

        self.nlp = spacy.load(spacy_dict)

    def do(self, paragraphs: List[dict]):
        if paragraphs == _termination_obj:
            return _termination_obj
        else:
            tokenized = []
            for paragraph in paragraphs:
                tokenized.append(
                    {
                        "url": paragraph["url"],
                        "paragraph_id": paragraph["paragraph_id"],
                        "tokens": self.nlp(paragraph["paragraph"]),
                    }
                )

            return tokenized


def extract_entities(tokens):
    if tokens == _termination_obj:
        return _termination_obj
    else:
        paragraph_entities = []
        for token_paragraph in tokens:
            entities = token_paragraph["tokens"].ents
            for entity in entities:
                paragraph_entities.append(
                    {
                        "url": token_paragraph["url"],
                        "paragraph_id": token_paragraph["paragraph_id"],
                        "entity": entity.ents,
                    }
                )
        return paragraph_entities


def enrich_entities(entities):
    if entities == _termination_obj:
        return _termination_obj
    else:
        enriched_entities = []
        for entity in entities["entity"]:
            enriched_entities.append(
                {
                    "url": entities["url"],
                    "paragraph_id": entities["paragraph_id"],
                    "entity_text": entity.text,
                    "entity_start_char": entity.start_char,
                    "entity_end_char": entity.end_char,
                    "entity_label": entity.label_,
                }
            )
        return enriched_entities
