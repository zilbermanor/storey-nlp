from typing import Union, List, Tuple
from storey.dtypes import _termination_obj
from storey.sources import _IterableSource
from storey import Event
import aiofiles
from datetime import datetime

import aiobotocore
from urllib.parse import urlparse


class S3Reader(_IterableSource):
    def __init__(
        self,
        urls: Union[List[str], str],
        aws_region: str,
        access_key_secret: str,
        access_key_id: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        if isinstance(urls, str):
            urls = [urls]
        self._urls = urls

        # Start S3 Client
        session = aiobotocore.get_session()
        client = session.create_client(
            "s3",
            region_name=aws_region,
            aws_secret_access_key=access_key_secret,
            aws_access_key_id=access_key_id,
        )
        self.client = client

    def _parse_url(self, url: str) -> Tuple[str, str]:
        parsed_url = urlparse(url, allow_fragments=False)
        return parsed_url.netloc, parsed_url.path

    async def _run_loop(self):
        async with self.client as client:
            for url in self._urls:
                bucket, key = self._parse_url(url)
                response = await client.get_object(bucket, key)
                async with response["Body"] as json_file:
                    paragraphs = await json_file.readlines()
                    for paragraph in paragraphs:
                        await self._do_downstream(
                            Event(paragraph, key=url, time=datetime.now())
                        )
            return await self._do_downstream(_termination_obj)


class ReadJSON(_IterableSource):
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
