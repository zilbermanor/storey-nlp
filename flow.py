from storey import build_flow, Source, Map, Filter, FlatMap
from storey.dtypes import _termination_obj
from flows import (
    URLDownloader,
    apply_nlp,
    extract_entities,
    enrich_entities,
    to_paragraphs,
)
import os
import cloudpickle

secrets = cloudpickle.load(open("secrets.pkl", "rb"))
os.environ.update(secrets)

flow = build_flow(
    [
        Source(),
        URLDownloader(secrets=secrets),  # Get URL and download
        FlatMap(to_paragraphs),  # extracts Disct[...list_path_in_json...] to List
        apply_nlp(spacy_dict="en_core_web_sm"),
        FlatMap(extract_entities),
        Map(enrich_entities),
        Filter(lambda ent: ent[0]["entity_label"] in ["PERSON", "ORG"]),
        Map(print)
        # Reduce({}, lambda acc, x: append_and_return(acc, x), full_event=True),
    ]
).run()


flow.emit("s3://igz-downloads/1.json")
flow.emit("s3://igz-downloads/2.json")
flow.emit(["s3://igz-downloads/2.json", "s3://igz-downloads/1.json"])
flow.emit("https://igz-downloads.s3.amazonaws.com/2.json")
flow.emit(_termination_obj)
