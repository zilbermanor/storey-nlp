from v3io.dataplane import Client
import argparse
import pprint
import json
from time import sleep


class StreamWatcher:
    def __init__(
        self, container: str, path: str, shard_id: int, seek_to: str,
    ):
        self.container = container
        self.path = path = path
        self.shard_id = shard_id
        self.seek_to = seek_to
        self.client: Client = Client()

        response = self.client.stream.seek(container, path, shard_id, seek_to)
        self.location = response.output.location
        print(f'got {self.location} with {response.status_code}')
        print(self.__dict__)

    def watch(self):
        response = self.client.stream.get_records(
            self.container, self.path, self.shard_id, self.location
        )
        response.raise_for_status()
        self.location = response.output.next_location
        for response_record in response.output.records:
            print(f"{self.container}/{self.path}:{self.shard_id} (#{response_record.sequence_number}) >> ")
            pprint.pprint(json.loads(response_record.data))


class CreateWatcherAction(argparse.Action):
    def __call__(self, parser, namespace, values: list, option_string=None):
        params = [
            "container",
            "path",
            "shard_id",
            "seek_to",
        ]

        defaults = {"seek_to": "earliest"}

        mapping = {}
        for i, param in enumerate(params):
            mapping[param] = values[i] if len(values) >= i else defaults[param]

        watchers.append(StreamWatcher(**mapping))


watchers = []

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--stream",
        action=CreateWatcherAction,
        default=[],
        nargs=4,
        help="""Adds a stream to the watch list. The stream should be given as
                --stream <container> <path> <shard_id> <seek_to>
                <container>: str
                <path>: str
                <shard_id>: int
                <seek_to>: EARLIEST / LATEST""",
    )

    parser.add_argument(
        "polling_interval",
        type=int,
        default="1",
        help="Polling interval in seconds",
    )

    args = parser.parse_args()

    while True:
        for watcher in watchers:
            watcher.watch()
        sleep(args.polling_interval)
