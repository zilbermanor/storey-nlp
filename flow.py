import json
from pprint import pprint
import spacy
from typing import Union, List, Optional
from datetime import datetime, timezone
import aiofiles
import storey
from storey import build_flow, Source, Map, Filter, FlatMap, Reduce, Event
from storey.dtypes import _termination_obj


class ReadJSON(storey.sources._IterableSource):
    """"""

    def __init__(self, paths: Union[List[str], str], **kwargs):
        super().__init__(**kwargs)
        if isinstance(paths, str):
            paths = [paths]
        self._paths = paths

    async def _run_loop(self):
        for path in self._paths:
            async with aiofiles.open(path, mode="r") as f:
                paragraphs = await f.readlines()
                for paragraph in paragraphs:
                    await self._do_downstream(
                        Event(paragraph, key=path.split("/")[-1], time=datetime.now())
                    )
        return await self._do_downstream(_termination_obj)


def append_and_return(dct, x):
    temp = dct.get(x.id, [])
    dct[x.id] = temp + [x.body]
    return dct


nlp = spacy.load("en_core_web_sm")
flow = build_flow(
    [
        ReadJSON(["data/2.json", "data/1.json"]),
        Map(lambda paragraph: nlp(paragraph)),
        FlatMap(lambda doc: doc.ents),
        Map(lambda ent: [ent.text, ent.start_char, ent.end_char, ent.label_]),
        Filter(lambda ent: ent[3] in ["PERSON", "ORG"]),
        Reduce({}, lambda acc, x: append_and_return(acc, x), full_event=True)
        # SendToHttp()
    ]
).run()

result = flow.await_termination()
pprint(result)
