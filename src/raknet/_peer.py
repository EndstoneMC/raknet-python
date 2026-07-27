from __future__ import annotations

import queue
import socket
import threading
import time
import traceback
from collections import deque
from dataclasses import dataclass
from typing import Callable, Iterator, Sequence

from raknet import raw
from raknet._router import Router
from raknet.exceptions import (
    ConnectError,
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
    RakNetError,
    StartupError,
)

_USER_MESSAGE_ID = int(raw.DefaultMessageIDTypes.ID_USER_PACKET_ENUM)


def _to_address(address: raw.SystemAddress) -> tuple[str, int]:
    return address.to_string(False), address.get_port()


def _resolve(host: str, port: int) -> str:
    # RakNet's SystemAddress does not resolve names; resolve here so the pending key
    # matches the numeric address RakNet reports back and RakNet skips its own blocking DNS.
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_DGRAM)
    infos.sort(key=lambda info: 0 if info[0] == socket.AF_INET else 1)
    return infos[0][4][0]


@dataclass(frozen=True)
class Pong:
    address: tuple[str, int]
    guid: int
    data: bytes
    round_trip_time: float


class _Mailbox:
    def __init__(self, maxsize: int | None = None) -> None:
        self._items: deque[bytes] = deque()
        self._cond = threading.Condition()
        self._exc: ConnectionClosed | None = None
        self._maxsize = maxsize

    def put(self, item: bytes) -> bool:
        with self._cond:
            if self._maxsize is not None and len(self._items) >= self._maxsize:
                return False
            self._items.append(item)
            self._cond.notify()
            return True

    def close(self, exc: ConnectionClosed) -> None:
        with self._cond:
            if self._exc is None:
                self._exc = exc
            self._cond.notify_all()

    def get(self, timeout: float | None = None) -> bytes:
        with self._cond:
            if not self._cond.wait_for(lambda: self._items or self._exc is not None, timeout):
                raise TimeoutError("recv timed out")
            if self._items:
                return self._items.popleft()
            assert self._exc is not None
            raise self._exc

    def poll(self, timeout: float = 0.0) -> bool:
        with self._cond:
            return self._cond.wait_for(lambda: bool(self._items) or self._exc is not None, timeout)


class _PendingConnect:
    __slots__ = ("event", "connection", "reason")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.connection: Connection | None = None
        self.reason: raw.DefaultMessageIDTypes | None = None


class _PendingPing:
    __slots__ = ("event", "pong", "started")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.pong: Pong | None = None
        self.started = time.monotonic()


class Connection:
    """A message-oriented connection to a single remote peer."""

    def __init__(self, peer: Peer, address: raw.SystemAddress, guid: raw.RakNetGUID) -> None:
        self._peer = peer
        self._raw_address = address
        self._raw_guid = guid
        self._closed = False
        self._owns_peer = False
        self._mailbox = self._make_mailbox()

    def _make_mailbox(self):
        return _Mailbox(self._peer._max_queue)

    @property
    def remote_address(self) -> tuple[str, int]:
        return _to_address(self._raw_address)

    @property
    def guid(self) -> int:
        return self._raw_guid.g

    @property
    def connected(self) -> bool:
        return not self._closed

    @property
    def latency(self) -> int:
        return self._peer._raw.get_last_ping(self._raw_guid)

    @property
    def average_latency(self) -> int:
        return self._peer._raw.get_average_ping(self._raw_guid)

    @property
    def lowest_latency(self) -> int:
        return self._peer._raw.get_lowest_ping(self._raw_guid)

    @property
    def statistics(self) -> raw.RakNetStatistics | None:
        return self._peer._raw.get_statistics(self._raw_address)

    def send(
        self,
        data: bytes,
        *,
        reliability: raw.PacketReliability = raw.PacketReliability.RELIABLE_ORDERED,
        priority: raw.PacketPriority = raw.PacketPriority.HIGH_PRIORITY,
        channel: int = 0,
    ) -> None:
        if self._closed:
            raise ConnectionClosed("connection is closed")
        payload = bytes([_USER_MESSAGE_ID]) + bytes(data)
        self._peer._raw.send(payload, priority, reliability, channel, self._raw_guid, False)

    def recv(self, timeout: float | None = None) -> bytes:
        return self._mailbox.get(timeout)

    def poll(self, timeout: float = 0.0) -> bool:
        return self._mailbox.poll(timeout)

    def close(self, *, notify: bool = True) -> None:
        if not self._closed:
            self._peer._close_connection(self, notify)
        if self._owns_peer:
            self._peer.close()

    def __iter__(self) -> Iterator[bytes]:
        while True:
            try:
                yield self.recv()
            except ConnectionClosedOK:
                return

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def __repr__(self) -> str:
        host, port = self.remote_address
        state = "open" if self.connected else "closed"
        return f"<{type(self).__name__} {host}:{port} guid={self.guid} {state}>"


class Peer:
    """A RakNet peer with a background receive pump. Can accept and initiate connections."""

    _connection_cls = Connection

    def __init__(
        self,
        addresses: Sequence[tuple[str, int]] = (("", 0),),
        *,
        max_connections: int = 32,
        max_incoming_connections: int | None = None,
        password: bytes | None = None,
        offline_ping_response: bytes = b"",
        max_queue: int | None = None,
    ) -> None:
        descriptors = [raw.SocketDescriptor(port, host) for host, port in addresses]
        self._raw = raw.RakPeer()
        self._closed = False
        self._max_queue = max_queue
        result = self._raw.startup(max_connections, descriptors)
        if result != raw.StartupResult.RAKNET_STARTED:
            raise StartupError(result)
        self._raw.set_maximum_incoming_connections(0 if max_incoming_connections is None else max_incoming_connections)
        if password is not None:
            self._raw.set_incoming_password(password)
        if offline_ping_response:
            self._raw.set_offline_ping_response(offline_ping_response)
        self._lock = threading.Lock()
        self._connections: dict[int, Connection] = {}
        self._pending_connects: dict[str, _PendingConnect] = {}
        self._pending_pings: dict[str, _PendingPing] = {}
        self._accept_queue = self._make_accept_queue()
        self._router = Router(self)
        self._pump = threading.Thread(target=self._pump_loop, name="raknet-pump", daemon=True)
        self._pump.start()

    def _make_accept_queue(self):
        return queue.SimpleQueue()

    def _make_pending_connect(self):
        return _PendingConnect()

    def _make_pending_ping(self):
        return _PendingPing()

    @property
    def guid(self) -> int:
        return self._raw.get_my_guid().g

    @property
    def local_address(self) -> tuple[str, int]:
        return _to_address(self._raw.get_my_bound_address())

    @property
    def connections(self) -> list[Connection]:
        with self._lock:
            return list(self._connections.values())

    @property
    def offline_ping_response(self) -> bytes:
        return self._raw.get_offline_ping_response()

    @offline_ping_response.setter
    def offline_ping_response(self, data: bytes) -> None:
        self._raw.set_offline_ping_response(data)

    @property
    def max_incoming_connections(self) -> int:
        return self._raw.get_maximum_incoming_connections()

    @max_incoming_connections.setter
    def max_incoming_connections(self, value: int) -> None:
        self._raw.set_maximum_incoming_connections(value)

    def accept(self, timeout: float | None = None) -> Connection:
        self._check_open()
        try:
            connection = self._accept_queue.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError("accept timed out") from None
        if connection is None:
            self._accept_queue.put(None)
            raise RakNetError("peer is closed")
        return connection

    def serve_forever(self, handler: Callable[[Connection], object]) -> None:
        while True:
            try:
                connection = self.accept()
            except RakNetError:
                return
            threading.Thread(target=self._run_handler, args=(handler, connection), daemon=True).start()

    def _run_handler(self, handler: Callable[[Connection], object], connection: Connection) -> None:
        try:
            handler(connection)
        except ConnectionClosed:
            pass
        finally:
            connection.close()

    def connect(
        self,
        address: tuple[str, int],
        *,
        password: bytes | None = None,
        timeout: float | None = None,
        attempts: int = 12,
        attempt_interval: float = 0.5,
    ) -> Connection:
        pending, key, ip = self._start_connect(address, password, attempts, attempt_interval)
        if not pending.event.wait(timeout):
            self._abandon_connect(key, ip, address[1])
            host, port = address
            raise TimeoutError(f"connect to {host}:{port} timed out")
        return self._finish_connect(address, pending)

    def ping(self, address: tuple[str, int], *, timeout: float | None = 1.0) -> Pong:
        pending, key = self._start_ping(address)
        if not pending.event.wait(timeout):
            with self._lock:
                self._pending_pings.pop(key, None)
            host, port = address
            raise TimeoutError(f"ping to {host}:{port} timed out")
        return self._finish_ping(pending)

    def ban(self, ip: str, duration: float | None = None) -> None:
        self._raw.add_to_ban_list(ip, 0 if duration is None else int(duration * 1000))

    def unban(self, ip: str) -> None:
        self._raw.remove_from_ban_list(ip)

    def clear_bans(self) -> None:
        self._raw.clear_ban_list()

    def close(self, timeout: float = 0.5) -> None:
        state = self._begin_close()
        if state is None:
            return
        self._pump.join()
        self._raw.shutdown(int(timeout * 1000))
        self._finish_close(*state)

    def __enter__(self) -> Peer:
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def __repr__(self) -> str:
        host, port = self.local_address if not self._closed else ("", 0)
        state = "closed" if self._closed else f"{host}:{port}"
        return f"<{type(self).__name__} {state} connections={len(self._connections)}>"

    @property
    def raw(self) -> raw.RakPeer:
        return self._raw

    def _check_open(self) -> None:
        if self._closed:
            raise RakNetError("peer is closed")

    def _pump_loop(self) -> None:
        try:
            while not self._closed:
                packet = self._raw.receive()
                if packet is None:
                    time.sleep(0.001)
                    continue
                self._router.route(packet)
        except BaseException:
            self._fail(traceback.format_exc())
            raise

    def _fail(self, detail: str) -> None:
        state = self._begin_close()
        if state is None:
            return
        self._raw.shutdown(0)
        self._finish_close(*state, exc=ConnectionClosedError(f"receive pump crashed:\n{detail}"))

    def _start_connect(self, address, password, attempts, attempt_interval):
        self._check_open()
        if attempts < 3:
            raise ValueError("attempts must be at least 3, RakNet probes one MTU size per third of the attempts")
        host, port = address
        try:
            ip = _resolve(host, port)
        except OSError:
            raise ConnectError(address, raw.ConnectionAttemptResult.CANNOT_RESOLVE_DOMAIN_NAME) from None
        pending = self._make_pending_connect()
        key = self._pending_key(ip, port)
        with self._lock:
            if self._closed:
                raise RakNetError("peer is closed")
            self._pending_connects[key] = pending
        result = self._raw.connect(
            ip,
            port,
            password or b"",
            send_connection_attempt_count=attempts,
            time_between_send_connection_attempts_ms=int(attempt_interval * 1000),
        )
        if result != raw.ConnectionAttemptResult.CONNECTION_ATTEMPT_STARTED:
            with self._lock:
                self._pending_connects.pop(key, None)
            raise ConnectError(address, result)
        return pending, key, ip

    def _abandon_connect(self, key, ip, port) -> None:
        with self._lock:
            self._pending_connects.pop(key, None)
        self._raw.cancel_connection_attempt(raw.SystemAddress(ip, port))

    def _finish_connect(self, address, pending) -> Connection:
        if pending.reason is not None:
            raise ConnectError(address, pending.reason)
        assert pending.connection is not None
        return pending.connection

    def _start_ping(self, address):
        self._check_open()
        host, port = address
        try:
            ip = _resolve(host, port)
        except OSError:
            raise RakNetError(f"cannot resolve {host}") from None
        pending = self._make_pending_ping()
        key = self._pending_key(ip, port)
        with self._lock:
            if self._closed:
                raise RakNetError("peer is closed")
            self._pending_pings[key] = pending
        if not self._raw.ping(ip, port, False):
            with self._lock:
                self._pending_pings.pop(key, None)
            raise RakNetError(f"ping to {host}:{port} failed")
        return pending, key

    def _finish_ping(self, pending) -> Pong:
        if pending.pong is None:
            raise RakNetError("peer is closed")
        return pending.pong

    def _begin_close(self):
        with self._lock:
            if self._closed:
                return None
            self._closed = True
            connections = list(self._connections.values())
            self._connections.clear()
            pending_connects = list(self._pending_connects.values())
            self._pending_connects.clear()
            pending_pings = list(self._pending_pings.values())
            self._pending_pings.clear()
        return connections, pending_connects, pending_pings

    def _finish_close(self, connections, pending_connects, pending_pings, exc: ConnectionClosed | None = None) -> None:
        if exc is None:
            exc = ConnectionClosedOK("peer closed")
        for connection in connections:
            connection._closed = True
            connection._mailbox.close(exc)
        for pending in pending_connects:
            pending.reason = raw.DefaultMessageIDTypes.ID_CONNECTION_ATTEMPT_FAILED
            pending.event.set()
        for pending in pending_pings:
            pending.event.set()
        self._accept_queue.put(None)

    def _pending_key(self, ip: str, port: int) -> str:
        return raw.SystemAddress(ip, port).to_string(True)

    def _pop_pending(self, table: dict, address: raw.SystemAddress):
        key = address.to_string(True)
        with self._lock:
            return table.pop(key, None)

    def _close_connection(self, connection: Connection, notify: bool) -> None:
        with self._lock:
            self._connections.pop(connection.guid, None)
        connection._closed = True
        if not self._closed:
            self._raw.close_connection(connection._raw_guid, notify)
        connection._mailbox.close(ConnectionClosedOK("connection closed"))

    # Sink callbacks, invoked from the pump thread.

    def incoming_connection(self, address: raw.SystemAddress, guid: raw.RakNetGUID) -> None:
        connection = self._connection_cls(self, address, guid)
        with self._lock:
            self._connections[guid.g] = connection
        self._accept_queue.put(connection)

    def connect_succeeded(self, address: raw.SystemAddress, guid: raw.RakNetGUID) -> None:
        pending = self._pop_pending(self._pending_connects, address)
        if pending is None:
            return
        connection = self._connection_cls(self, address, guid)
        with self._lock:
            self._connections[guid.g] = connection
        pending.connection = connection
        pending.event.set()

    def connect_failed(self, address: raw.SystemAddress, reason: raw.DefaultMessageIDTypes) -> None:
        pending = self._pop_pending(self._pending_connects, address)
        if pending is None:
            return
        pending.reason = reason
        pending.event.set()

    def message(self, guid: raw.RakNetGUID, data: bytes) -> None:
        with self._lock:
            connection = self._connections.get(guid.g)
        if connection is None:
            return
        if not connection._mailbox.put(data):
            self._overflow(connection)

    def _overflow(self, connection: Connection) -> None:
        with self._lock:
            self._connections.pop(connection.guid, None)
        connection._closed = True
        if not self._closed:
            self._raw.close_connection(connection._raw_guid, True)
        connection._mailbox.close(ConnectionClosedError("receive queue overflow"))

    def disconnected(self, guid: raw.RakNetGUID, graceful: bool) -> None:
        with self._lock:
            connection = self._connections.pop(guid.g, None)
        if connection is None:
            return
        connection._closed = True
        if graceful:
            connection._mailbox.close(ConnectionClosedOK("connection closed by remote"))
        else:
            connection._mailbox.close(ConnectionClosedError("connection lost"))

    def pong(self, address: raw.SystemAddress, guid: raw.RakNetGUID, data: bytes) -> None:
        pending = self._pop_pending(self._pending_pings, address)
        if pending is None:
            return
        pending.pong = Pong(_to_address(address), guid.g, data, time.monotonic() - pending.started)
        pending.event.set()


def create_server(
    address: tuple[str, int] = ("", 0),
    *,
    max_connections: int = 32,
    password: bytes | None = None,
    offline_ping_response: bytes = b"",
    max_queue: int | None = None,
) -> Peer:
    return Peer(
        [address],
        max_connections=max_connections,
        max_incoming_connections=max_connections,
        password=password,
        offline_ping_response=offline_ping_response,
        max_queue=max_queue,
    )


def create_connection(
    address: tuple[str, int],
    *,
    password: bytes | None = None,
    timeout: float | None = None,
    attempts: int = 12,
    attempt_interval: float = 0.5,
    max_queue: int | None = None,
) -> Connection:
    peer = Peer(max_connections=1, max_queue=max_queue)
    try:
        connection = peer.connect(
            address, password=password, timeout=timeout, attempts=attempts, attempt_interval=attempt_interval
        )
    except BaseException:
        peer.close(0)
        raise
    connection._owns_peer = True
    return connection


def ping(address: tuple[str, int], *, timeout: float | None = 1.0) -> Pong:
    peer = Peer(max_connections=1)
    try:
        return peer.ping(address, timeout=timeout)
    finally:
        peer.close(0)
