# storey-nlp
NLP Example flow based on mlrun/storey

The demo shows:
- Using storey classes
- Using MLRun's `flow` serving
- Defining and Deploying a `Graph` over 2 functions or more functions

Please run [nlp-pipe.ipynb](./nlp-pipe.ipynb)

## Stream Watcher
[Stream Watcher](./stream_watcher.py) utility was added.
To use please:
`python -m stream_watcher --stream <container> <stream_path> <shard_id> <seek_to (EARLIEST/LATEST)> <polling_interval in seconds>`

You can add multiple streams by specifying more `--stream <container> <stream_path> <shard_id> <seek_to (EARLIEST/LATEST)>`.

`V3IO_WEBAPI`, `V3IO_ACCESS_KEY`, `V3IO_USERNAME` will be taken from the available environment variables.