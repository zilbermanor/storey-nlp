from storey import MapClass, Event
from storey.dtypes import _termination_obj
from mlrun import get_object
from typing import Union, List
import json


class Message:
    def __init__(self, msg="", context=None, name=None):
        self.context = context
        self.name = name
        self.msg = msg

    def do(self, x):
        print("Messsage:", self.msg)
        self.context.logger.info(f'Messsage: {self.msg}')
        return x


class BaseClass:
    def __init__(self, context, state=None, name=None):
        self.context = context
        self.state = state
        self.name = name


class URLDownloader:
    def __init__(self, secrets: dict = None, **kwargs):
        self.secrets = secrets

    def do(self, urls: Union[str, Event, List]):
        url_list = []
        if urls == _termination_obj:
            return self._do_downstream(_termination_obj)
#         if type(urls) == bytes or type(urls) == str:
            
        else:
            url_list = urls.decode()
            if type(url_list) is not list:
                url_list = [url_list]
#             url_list = json.loads(url_list)
#             url_list = urls
        docs = []
        for url in url_list:
            try:
                doc = json.loads(get_object(url, self.secrets).decode("utf-8"))
                docs.append({"url": url, "doc": doc})
            except Exception as e:
                print(f"Couldnt get {url}, {e}")
        return docs


class ToParagraphs:
    def __init__(self, **kwargs):
        pass

    def do(self, docs: Union[List, Event]):
        if docs == _termination_obj or docs is None:
            print("No doc")
            return _termination_obj
        else:
            paragraphs = []
            if type(docs) is not list:
                docs = [docs]
            for doc in docs:
                for i, paragraph in enumerate(doc["doc"]):
                    paragraphs.append(
                        {"url": doc["url"], "paragraph_id": i, "paragraph": paragraph}
                    )
            return paragraphs


def to_paragraphs(docs: Union[List, Event]):
    if docs == _termination_obj or docs is None:
        print("No doc")
        return _termination_obj
    else:
        paragraphs = []
        for doc in docs:
            for i, paragraph in enumerate(doc["doc"]):
                paragraphs.append(
                    {"url": doc["url"], "paragraph_id": i, "paragraph": paragraph}
                )
        return paragraphs
