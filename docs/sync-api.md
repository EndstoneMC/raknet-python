# Sync API design

The pythonic layer that sits on top of the faithful `raknet._raknet` binding. This document fixes the shape of the sync API before implementation. The asyncio API will mirror it later under `raknet.asyncio`.

## Goals

- Feel like the standard library: `socket.create_server` / `socket.create_connection` naming, context managers, per-call timeouts raising the builtin `TimeoutError`, the `ConnectionError` exception family.
- Message-oriented connections like `multiprocessing.connection.Connection` and `websockets`: `send(bytes)` / `recv() -> bytes`, iteration yields messages, closed connections raise instead of returning sentinels.
- Keep the faithful layer fully accessible for anything the pythonic layer does not cover.
- Every blocking call has an obvious async twin, so `raknet.asyncio` is a rename away (the `zmq.asyncio` / `redis.asyncio` convention).

## Module layout

```
raknet/            sync API (this document) + shared enums re-exported
raknet.raw         the faithful binding, re-exported (today's raknet._raknet)
raknet.asyncio     async mirror (later)
```

```
src/raknet/
    __init__.py    public sync surface
    raw.py         `from raknet._raknet import *`
    _peer.py       Peer / Connection implementation
    _router.py     packet routing shared with the future asyncio layer
    exceptions.py
```

Top-level re-exports: `Peer`, `Connection`, `create_server`, `create_connection`, `ping`, the exceptions, and the two enums users need for `send()`: `PacketPriority`, `PacketReliability`.

## Exceptions

```
RakNetError(Exception)
├── StartupError(RakNetError)                       .result: raw.StartupResult
├── ConnectError(RakNetError, ConnectionError)      .reason: raw.ConnectionAttemptResult | raw.DefaultMessageIDTypes
└── ConnectionClosed(RakNetError, ConnectionError)
    ├── ConnectionClosedOK                          graceful ID_DISCONNECTION_NOTIFICATION
    └── ConnectionClosedError                       ID_CONNECTION_LOST
```

- Timeouts raise the builtin `TimeoutError` (what `socket` has aliased since 3.10).
- `ConnectError.reason` carries the precise cause: an immediate `ConnectionAttemptResult` (e.g. `ALREADY_CONNECTED_TO_ENDPOINT`) or the failure message that came back (`ID_CONNECTION_ATTEMPT_FAILED`, `ID_INVALID_PASSWORD`, `ID_NO_FREE_INCOMING_CONNECTIONS`, `ID_INCOMPATIBLE_PROTOCOL_VERSION`, `ID_CONNECTION_BANNED`, `ID_IP_RECENTLY_CONNECTED`, `ID_ALREADY_CONNECTED`).
- The `ConnectionClosedOK` / `ConnectionClosedError` split follows `websockets`; both subclass `ConnectionError` so generic `except ConnectionError` works.

## Addresses

Public API speaks `(host, port)` tuples like the standard library. `raw.SystemAddress` is accepted anywhere a tuple is, and `Connection.remote_address` returns a tuple. GUIDs are plain `int`.

## Peer

```python
class Peer:
    def __init__(
        self,
        addresses: Sequence[tuple[str, int]] = (("", 0),),   # local bind addresses
        *,
        max_connections: int = 32,
        max_incoming_connections: int | None = None,          # None: 0 for pure clients, max_connections for create_server
        password: bytes | None = None,
        offline_ping_response: bytes = b"",
    )
```

The constructor starts the peer (`startup`) and the receive pump; `StartupError` on failure. `Peer` is a context manager; `close()` shuts down.

| Member | Behaviour |
| --- | --- |
| `accept(timeout=None) -> Connection` | Blocks for the next incoming connection (`ID_NEW_INCOMING_CONNECTION`). |
| `serve_forever(handler)` | Accepts until the peer is closed, running `handler(connection)` in a daemon thread per connection; closes the connection when the handler returns and swallows `ConnectionClosed` raised by it. |
| `connect(address, *, password=None, timeout=None, attempts=12, attempt_interval=0.5) -> Connection` | Blocks until accepted; raises `ConnectError` / `TimeoutError`. |
| `connections -> list[Connection]` | Live connections, incoming and outgoing. |
| `ping(address, *, timeout=None) -> Pong` | Unconnected ping; `Pong(address, guid, data, round_trip_time)`. |
| `ban(ip, duration=None)` / `unban(ip)` / `clear_bans()` | `duration` in seconds, `None` = forever. |
| `close(timeout=0.5)` | Notifies peers, blocks up to `timeout` to flush, stops the pump. |
| `guid -> int`, `local_address -> tuple[str, int]` | Read-only. |
| `offline_ping_response -> bytes` | Read/write property. |
| `max_incoming_connections -> int` | Read/write property. |
| `raw -> raw.RakPeer` | Escape hatch to the faithful layer. |

Module-level helpers, named after `socket`:

```python
def create_server(address=("", 0), *, max_connections=32, password=None, ...) -> Peer
def create_connection(address, *, password=None, timeout=None, ...) -> Connection
def ping(address, *, timeout=1.0) -> Pong        # ephemeral peer, one unconnected ping
```

`create_connection` builds a single-connection `Peer` behind the scenes; closing the returned `Connection` also closes that peer (like the socket returned by `socket.create_connection`).

## Connection

```python
class Connection:
    remote_address: tuple[str, int]
    guid: int
    connected: bool
    latency: int                    # last ping, ms (average_latency, lowest_latency also available)
    statistics: raw.RakNetStatistics | None

    def send(self, data: bytes, *,
             reliability: PacketReliability = RELIABLE_ORDERED,
             priority: PacketPriority = HIGH_PRIORITY,
             channel: int = 0) -> None
    def recv(self, timeout: float | None = None) -> bytes
    def poll(self, timeout: float = 0.0) -> bool          # like multiprocessing.Connection.poll
    def close(self, *, notify: bool = True) -> None
    def __iter__(self) -> Iterator[bytes]                 # yields messages until closed
    # context manager
```

Semantics:

- Message framing: none. `send` transmits the payload verbatim and `recv` returns it verbatim. RakNet reserves first bytes below `ID_USER_PACKET_ENUM` for its own control traffic and only delivers packets whose first byte is a user message id, so a payload must lead with an id `>= ID_USER_PACKET_ENUM` (Minecraft Bedrock uses `0xFE`). The router treats every such packet as a message and everything below the threshold as a lifecycle event.
- `recv` on a closed connection raises `ConnectionClosedOK` / `ConnectionClosedError`; messages already queued are still delivered first. Iteration ends cleanly on `ConnectionClosedOK` and propagates `ConnectionClosedError`.
- `send` after close raises `ConnectionClosed`. `send` never blocks (RakNet buffers); ACK receipts are out of scope for v1.
- `close(notify=True)` sends the disconnection notification; `notify=False` drops silently.

## Pump and asyncio forward-compatibility

One daemon thread per `Peer` polls `raw_peer.receive()` (the binding already releases the GIL) and hands each packet to a router:

- user packets → per-connection `queue.SimpleQueue`
- connection lifecycle messages → resolve pending `connect()` calls, feed `accept()`, mark connections closed
- unconnected pongs → resolve pending `ping()` calls by address

The router (`_router.py`) is written against two tiny interfaces: a per-connection mailbox (put/get/close) and a waker. The sync layer instantiates it with `queue` + `threading.Event`. The asyncio layer will reuse the same pump thread and router, with mailboxes that hand off via `loop.call_soon_threadsafe` into `asyncio.Queue` — the public async API then mirrors this document with `await` in front (`await peer.accept()`, `async for message in conn`, timeouts left to `asyncio.timeout()`).

## Examples

Echo server:

```python
import raknet

def handler(conn):
    for message in conn:
        conn.send(message)

with raknet.create_server(("0.0.0.0", 60000), max_connections=32) as server:
    server.serve_forever(handler)
```

Client:

```python
with raknet.create_connection(("127.0.0.1", 60000), timeout=5) as conn:
    conn.send(b"hello")
    print(conn.recv(timeout=5))
```

Server-list ping:

```python
pong = raknet.ping(("play.example.com", 19132))
print(pong.data, pong.round_trip_time)
```

## Out of scope for v1

- ACK receipts (`send(..., receipt=True)` returning a waitable).
- `advertise_system` / out-of-band messaging sugar.
- Security (`initialize_security`) — LIBCAT is compiled out.
