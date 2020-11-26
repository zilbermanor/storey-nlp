from pprint import pprint
import spacy
from storey import build_flow, Source, Map, Filter, FlatMap, Reduce, Event
from storey.dtypes import _termination_obj

from sources import ReadJSON
from flows import URLDownloader
import os
from typing import Union, List
import cloudpickle


def append_and_return(dct, x):
    temp = dct.get(x.id, [])
    dct[x.id] = temp + [x.body]
    return dct


def flow_v1():
    flow = build_flow(
        [
            ReadJSON(["data/2.json", "data/1.json"]),
            Map(lambda paragraph: nlp(paragraph)),
            FlatMap(lambda doc: doc.ents),
            Map(lambda ent: [ent.text, ent.start_char, ent.end_char, ent.label_]),
            # Filter(lambda ent: ent[3] in ["PERSON", "ORG"]),
            Reduce({}, lambda acc, x: append_and_return(acc, x), full_event=True)
            # SendToHttp()
        ]
    ).run()

    return flow


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


def apply_nlp(paragraphs: List[dict]):
    if paragraphs == _termination_obj:
        return _termination_obj
    else:
        tokenized = []
        for paragraph in paragraphs:
            tokenized.append(
                {
                    "url": paragraph["url"],
                    "paragraph_id": paragraph["paragraph_id"],
                    "tokens": nlp(paragraph["paragraph"]),
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


nlp = spacy.load("en_core_web_sm")
secrets = cloudpickle.load(open("secrets.pkl", "rb"))
os.environ.update(secrets)

flow = build_flow(
    [
        Source(),
        URLDownloader(secrets=secrets),  # Get URL and download
        FlatMap(to_paragraphs),  # extracts Disct[...list_path_in_json...] to List
        Map(apply_nlp),
        FlatMap(extract_entities),
        Map(enrich_entities),
        Filter(lambda ent: ent[0]["entity_label"] in ["PERSON", "ORG"]),
        Map(print)
        # Reduce({}, lambda acc, x: append_and_return(acc, x), full_event=True),
    ]
).run()


flow.emit("s3://igz-downloads/1.json")
flow.emit("s3://igz-downloads/2.json")
# flow.emit(["s3://igz-downloads/2.json", "s3://igz-downloads/1.json"])
flow.emit("https://igz-downloads.s3.amazonaws.com/2.json")
flow.emit(_termination_obj)
