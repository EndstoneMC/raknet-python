"""Asyncio mirror of the sync API. Timeouts are left to asyncio.timeout()."""

from __future__ import annotations

import asyncio
import time
from typing import AsyncIterator, Awaitable, Callable, Sequence

from raknet import _peer, raw
from raknet._peer import Pong
from raknet.exceptions import ConnectionClosed, ConnectionClosedOK, RakNetError

__all__ = ["Connection", "Peer", "Pong", "create_connection", "create_server", "ping"]

_CLOSED = object()


class _AsyncMailbox:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._queue: asyncio.Queue = asyncio.Queue()
        self._exc = None
        self._drained = False

    def put(self, item: bytes) -> None:
        try:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, item)
        except RuntimeError:
            pass

    def close(self, exc) -> None:
        def _close():
            if self._exc is None:
                self._exc = exc
                self._queue.put_nowait(_CLOSED)

        try:
            self._loop.call_soon_threadsafe(_close)
        except RuntimeError:
            pass

    async def get(self) -> bytes:
        if self._drained:
            raise self._exc
        item = await self._queue.get()
        if item is _CLOSED:
            self._drained = True
            self._queue.put_nowait(_CLOSED)
            raise self._exc
        return item


class _ThreadsafeEvent:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._event = asyncio.Event()

    def set(self) -> None:
        try:
            self._loop.call_soon_threadsafe(self._event.set)
        except RuntimeError:
            pass

    async def wait(self) -> None:
        await self._event.wait()


class _AsyncPendingConnect:
    __slots__ = ("event", "connection", "reason")

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.event = _ThreadsafeEvent(loop)
        self.connection: Connection | None = None
        self.reason: raw.DefaultMessageIDTypes | None = None


class _AsyncPendingPing:
    __slots__ = ("event", "pong", "started")

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.event = _ThreadsafeEvent(loop)
        self.pong: Pong | None = None
        self.started = time.monotonic()


class _AsyncAcceptQueue:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._queue: asyncio.Queue = asyncio.Queue()

    def put(self, item) -> None:
        try:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, item)
        except RuntimeError:
            pass

    async def get(self):
        return await self._queue.get()


class Connection(_peer.Connection):
    """A message-oriented connection to a single remote peer, asyncio flavour."""

    _peer: Peer

    def _make_mailbox(self):
        return _AsyncMailbox(self._peer._loop)

    async def recv(self) -> bytes:
        return await self._mailbox.get()

    def poll(self, timeout: float = 0.0) -> bool:
        raise NotImplementedError("poll is not available in the asyncio API, use recv with asyncio.timeout")

    async def send(
        self,
        data: bytes,
        *,
        reliability: raw.PacketReliability = raw.PacketReliability.RELIABLE_ORDERED,
        priority: raw.PacketPriority = raw.PacketPriority.HIGH_PRIORITY,
        channel: int = 0,
    ) -> None:
        _peer.Connection.send(self, data, reliability=reliability, priority=priority, channel=channel)

    async def close(self, *, notify: bool = True) -> None:
        if not self._closed:
            self._peer._close_connection(self, notify)
        if self._owns_peer:
            await self._peer.close()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            try:
                yield await self.recv()
            except ConnectionClosedOK:
                return

    async def __aenter__(self) -> Connection:
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

    def __iter__(self):
        raise TypeError("use 'async for' with raknet.asyncio.Connection")

    def __enter__(self):
        raise TypeError("use 'async with' with raknet.asyncio.Connection")


class Peer(_peer.Peer):
    """A RakNet peer with a background receive pump, asyncio flavour."""

    _connection_cls = Connection

    def __init__(
        self,
        addresses: Sequence[tuple[str, int]] = (("", 0),),
        *,
        max_connections: int = 32,
        max_incoming_connections: int | None = None,
        password: bytes | None = None,
        offline_ping_response: bytes = b"",
    ) -> None:
        self._loop = asyncio.get_running_loop()
        super().__init__(
            addresses,
            max_connections=max_connections,
            max_incoming_connections=max_incoming_connections,
            password=password,
            offline_ping_response=offline_ping_response,
        )

    def _make_accept_queue(self):
        return _AsyncAcceptQueue(self._loop)

    def _make_pending_connect(self):
        return _AsyncPendingConnect(self._loop)

    def _make_pending_ping(self):
        return _AsyncPendingPing(self._loop)

    async def accept(self) -> Connection:
        self._check_open()
        connection = await self._accept_queue.get()
        if connection is None:
            self._accept_queue.put(None)
            raise RakNetError("peer is closed")
        return connection

    async def serve_forever(self, handler: Callable[[Connection], Awaitable[object]]) -> None:
        tasks: set[asyncio.Task] = set()
        while True:
            try:
                connection = await self.accept()
            except RakNetError:
                return
            task = asyncio.create_task(self._run_handler(handler, connection))
            tasks.add(task)
            task.add_done_callback(tasks.discard)

    async def _run_handler(self, handler: Callable[[Connection], Awaitable[object]], connection: Connection) -> None:
        try:
            await handler(connection)
        except ConnectionClosed:
            pass
        finally:
            await connection.close()

    async def connect(
        self,
        address: tuple[str, int],
        *,
        password: bytes | None = None,
        attempts: int = 12,
        attempt_interval: float = 0.5,
    ) -> Connection:
        pending, key, ip = await asyncio.to_thread(self._start_connect, address, password, attempts, attempt_interval)
        try:
            await pending.event.wait()
        except asyncio.CancelledError:
            self._abandon_connect(key, ip, address[1])
            raise
        return self._finish_connect(address, pending)

    async def ping(self, address: tuple[str, int]) -> Pong:
        pending, key = await asyncio.to_thread(self._start_ping, address)
        try:
            await pending.event.wait()
        except asyncio.CancelledError:
            with self._lock:
                self._pending_pings.pop(key, None)
            raise
        return self._finish_ping(pending)

    async def close(self, timeout: float = 0.5) -> None:
        state = self._begin_close()
        if state is None:
            return
        await asyncio.to_thread(self._pump.join)
        await asyncio.to_thread(self._raw.shutdown, int(timeout * 1000))
        self._finish_close(*state)

    async def __aenter__(self) -> Peer:
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

    def __enter__(self):
        raise TypeError("use 'async with' with raknet.asyncio.Peer")


async def create_server(
    address: tuple[str, int] = ("", 0),
    *,
    max_connections: int = 32,
    password: bytes | None = None,
    offline_ping_response: bytes = b"",
) -> Peer:
    return Peer(
        [address],
        max_connections=max_connections,
        max_incoming_connections=max_connections,
        password=password,
        offline_ping_response=offline_ping_response,
    )


async def create_connection(
    address: tuple[str, int],
    *,
    password: bytes | None = None,
    attempts: int = 12,
    attempt_interval: float = 0.5,
) -> Connection:
    peer = Peer(max_connections=1)
    try:
        connection = await peer.connect(
            address, password=password, attempts=attempts, attempt_interval=attempt_interval
        )
    except BaseException:
        await peer.close(0)
        raise
    connection._owns_peer = True
    return connection


async def ping(address: tuple[str, int]) -> Pong:
    peer = Peer(max_connections=1)
    try:
        return await peer.ping(address)
    finally:
        await peer.close(0)
