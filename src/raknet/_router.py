from __future__ import annotations

from typing import Protocol

from raknet import raw

_MSG = raw.DefaultMessageIDTypes

CONNECT_FAILURES = frozenset(
    int(mid)
    for mid in (
        _MSG.ID_CONNECTION_ATTEMPT_FAILED,
        _MSG.ID_ALREADY_CONNECTED,
        _MSG.ID_NO_FREE_INCOMING_CONNECTIONS,
        _MSG.ID_CONNECTION_BANNED,
        _MSG.ID_INVALID_PASSWORD,
        _MSG.ID_INCOMPATIBLE_PROTOCOL_VERSION,
        _MSG.ID_IP_RECENTLY_CONNECTED,
    )
)


class Sink(Protocol):
    def incoming_connection(self, address: raw.SystemAddress, guid: raw.RakNetGUID) -> None: ...

    def connect_succeeded(self, address: raw.SystemAddress, guid: raw.RakNetGUID) -> None: ...

    def connect_failed(self, address: raw.SystemAddress, reason: raw.DefaultMessageIDTypes) -> None: ...

    def message(self, guid: raw.RakNetGUID, data: bytes) -> None: ...

    def disconnected(self, guid: raw.RakNetGUID, graceful: bool) -> None: ...

    def pong(self, address: raw.SystemAddress, guid: raw.RakNetGUID, data: bytes) -> None: ...


class Router:
    """Demultiplexes raw packets into sink callbacks. Shared by the sync and asyncio layers."""

    def __init__(self, sink: Sink) -> None:
        self._sink = sink

    def route(self, packet: raw.Packet) -> None:
        data = packet.data
        if not data:
            return
        mid = data[0]
        if mid >= int(_MSG.ID_USER_PACKET_ENUM):
            self._sink.message(packet.guid, data)
        elif mid == int(_MSG.ID_NEW_INCOMING_CONNECTION):
            self._sink.incoming_connection(packet.system_address, packet.guid)
        elif mid == int(_MSG.ID_CONNECTION_REQUEST_ACCEPTED):
            self._sink.connect_succeeded(packet.system_address, packet.guid)
        elif mid in CONNECT_FAILURES:
            self._sink.connect_failed(packet.system_address, _MSG(mid))
        elif mid == int(_MSG.ID_DISCONNECTION_NOTIFICATION):
            self._sink.disconnected(packet.guid, graceful=True)
        elif mid == int(_MSG.ID_CONNECTION_LOST):
            self._sink.disconnected(packet.guid, graceful=False)
        elif mid == int(_MSG.ID_UNCONNECTED_PONG):
            self._sink.pong(packet.system_address, packet.guid, data[5:])
