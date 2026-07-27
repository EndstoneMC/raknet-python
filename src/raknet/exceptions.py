from __future__ import annotations


class RakNetError(Exception):
    """Base class for all raknet errors."""


class StartupError(RakNetError):
    """The peer could not be started."""

    def __init__(self, result):
        super().__init__(f"startup failed: {result.name}")
        self.result = result


class ConnectError(RakNetError, ConnectionError):
    """A connection attempt was rejected or failed."""

    def __init__(self, address, reason):
        host, port = address
        super().__init__(f"connect to {host}:{port} failed: {reason.name}")
        self.reason = reason


class ConnectionClosed(RakNetError, ConnectionError):
    """The connection is closed."""


class ConnectionClosedOK(ConnectionClosed):
    """The connection was closed with a disconnection notification."""


class ConnectionClosedError(ConnectionClosed):
    """The connection was lost without a disconnection notification."""
