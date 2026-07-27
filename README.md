# raknet

[![PyPI](https://img.shields.io/pypi/v/raknet.svg)](https://pypi.org/project/raknet/)
[![Python](https://img.shields.io/pypi/pyversions/raknet.svg)](https://pypi.org/project/raknet/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](LICENSE)

Python bindings for [RakNet](https://github.com/facebookarchive/RakNet), the UDP networking
library used by Minecraft Bedrock. It ships a faithful binding of RakNet's public API and a
pythonic layer on top of it, with both a synchronous and an asyncio interface.

```python
import raknet

with raknet.create_connection(("play.example.com", 19132), timeout=5) as conn:
    conn.send(b"\xfe...")          # a Bedrock game packet
    print(conn.recv(timeout=5))
```

## Installation

```shell
pip install raknet
```

Requires Python 3.12 or newer. Prebuilt `abi3` wheels are published for Windows and Linux
(x86-64); a single wheel works across all supported Python versions.

## The three layers

| Module | Interface | Use it for |
| --- | --- | --- |
| `raknet` | synchronous | blocking servers and clients, threads |
| `raknet.asyncio` | asyncio | event-loop applications; mirrors the sync API with `await` |
| `raknet.raw` | faithful C++ binding | direct access to `RakPeerInterface` and every RakNet type |

The two high-level layers share one connection model: a `Peer` accepts and initiates
connections, and each `Connection` is a message pipe with `send` / `recv`. Anything the
high-level layer does not cover is reachable through `peer.raw`.

## Quick start

### Server

```python
import raknet

def handle(connection):
    for message in connection:      # iterates until the peer disconnects
        connection.send(message)    # echo it back

with raknet.create_server(("0.0.0.0", 19132), max_connections=32) as server:
    server.serve_forever(handle)    # one thread per connection
```

### Client

```python
import raknet

with raknet.create_connection(("127.0.0.1", 19132), timeout=5) as conn:
    conn.send(b"\xfehello")
    reply = conn.recv(timeout=5)
```

### asyncio

The asyncio layer is the same API with `await` in front; per-call timeouts are left to
`asyncio.timeout()`.

```python
import asyncio
import raknet.asyncio

async def handle(connection):
    async for message in connection:
        await connection.send(message)

async def main():
    server = await raknet.asyncio.create_server(("0.0.0.0", 19132))
    async with server:
        await server.serve_forever(handle)

asyncio.run(main())
```

### Server-list ping

```python
pong = raknet.ping(("play.example.com", 19132), timeout=5)
print(pong.round_trip_time, pong.data)
```

More runnable programs are in [examples/](examples).

## Messages

`send` and `recv` move bytes verbatim; there is no added framing. RakNet reads the first byte
of every packet as a message id and only delivers packets whose id is a user id
(`>= 0x86`, `ID_USER_PACKET_ENUM`). Lower ids are consumed as RakNet's own control traffic, so
each payload must begin with a user id. Minecraft Bedrock uses `0xFE`. Reliability, priority
and the ordering channel are per-call:

```python
conn.send(data, reliability=raknet.PacketReliability.RELIABLE_ORDERED,
          priority=raknet.PacketPriority.HIGH_PRIORITY, channel=0)
```

## Errors

Failures raise exceptions rather than returning sentinels. All inherit from `RakNetError`, and
the connection-lifecycle ones also inherit from the builtin `ConnectionError`.

| Exception | Raised when |
| --- | --- |
| `StartupError` | the peer could not bind its socket |
| `ConnectError` | a connection attempt was rejected (`.reason` carries the cause) |
| `ConnectionClosedOK` | the remote disconnected gracefully |
| `ConnectionClosedError` | the connection was lost without notice |
| `TimeoutError` | a `timeout=` elapsed (the builtin) |

## Building from source

Building needs a C++17 compiler and [Conan](https://conan.io). The `raknet` recipe is hosted on
the `endstone` remote:

```shell
conan remote add endstone https://conan.cloudsmith.io/endstone/conan/
pip install .
```

The [conan-py-build](https://github.com/conan-io/conan-py-build) backend drives Conan and CMake,
so no separate `conan install` step is required.

## Development

```shell
pip install -e .
pytest
```

## License

BSD-3-Clause. See [LICENSE](LICENSE).

RakNet is © Oculus VR and released under a BSD-style license.
