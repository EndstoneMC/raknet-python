# Examples

Runnable examples for the `raknet` package. Install the package first
(`pip install raknet`), then run any script directly.

| Script | What it shows |
| --- | --- |
| [echo_server.py](echo_server.py) / [echo_client.py](echo_client.py) | Synchronous echo server and client. |
| [async_echo_server.py](async_echo_server.py) / [async_echo_client.py](async_echo_client.py) | The same over the asyncio API. |
| [server_list_ping.py](server_list_ping.py) | Query a server's MOTD with an unconnected ping. |

Start a server in one terminal and the matching client in another:

```shell
python echo_server.py
python echo_client.py
```

Every script takes `--host` and `--port` (see `--help`).
