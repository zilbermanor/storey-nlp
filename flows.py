from storey import MapClass, Event
from storey.dtypes import _termination_obj
from mlrun import get_object
from mlrun.utils import Logger
from typing import Union, List
import json


class URLDownloader(MapClass):
    def __init__(self, prefix: str = None, secrets: dict = None):

        super().__init__()

        # Set mlrun context
        # self.logger = Logger(1, name="URLDownloader")
        # self.logger.info("URLDownloader online")
        self.secrets = secrets

    def do(self, urls: Union[str, Event, List]):
        url_list = []
        if type(urls) == Event:
            url_list = urls.Body
        elif type(urls) == str:
            url_list = [urls]
        elif urls == _termination_obj:
            return self._do_downstream(_termination_obj)
        # self.logger.debug(f"Handling {urls}")
        docs = []
        for url in url_list:
            try:
                doc = json.loads(get_object(url, self.secrets).decode("utf-8"))
                docs.append({"url": url, "doc": doc})
            except Exception as e:
                print(f"Couldnt get {url}, {e}")
                # self.logger.warn(f"Couldnt get {url}, {e}")
        return docs
