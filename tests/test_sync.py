import socket
import threading

import pytest

import raknet

HOST = "127.0.0.1"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((HOST, 0))
        return sock.getsockname()[1]


def test_echo_roundtrip():
    with raknet.create_server((HOST, 0), max_connections=4) as server:

        def serve():
            conn = server.accept(timeout=5)
            for message in conn:
                conn.send(message)

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        with raknet.create_connection(server.local_address, timeout=5) as conn:
            assert conn.connected
            assert conn.remote_address == server.local_address
            conn.send(b"hello raknet")
            assert conn.recv(timeout=5) == b"hello raknet"
        thread.join(timeout=5)
        assert not thread.is_alive()


def test_offline_ping():
    with raknet.create_server((HOST, 0), offline_ping_response=b"MOTD") as server:
        pong = raknet.ping(server.local_address, timeout=5)
        assert pong.data == b"MOTD"
        assert pong.guid == server.guid
        assert pong.address == server.local_address
        assert 0 <= pong.round_trip_time < 5


def test_invalid_password():
    with raknet.create_server((HOST, 0), password=b"sekret") as server:
        with pytest.raises(raknet.ConnectError) as excinfo:
            raknet.create_connection(server.local_address, password=b"wrong", timeout=5)
        assert excinfo.value.reason == raknet.raw.DefaultMessageIDTypes.ID_INVALID_PASSWORD


def test_connect_failure_dead_port():
    with pytest.raises((raknet.ConnectError, TimeoutError)):
        raknet.create_connection((HOST, free_port()), timeout=5, attempts=3, attempt_interval=0.05)


def test_connect_rejects_too_few_attempts():
    with raknet.create_server((HOST, 0)) as server:
        with pytest.raises(ValueError):
            server.connect((HOST, free_port()), attempts=2)


def test_close_notification():
    with raknet.create_server((HOST, 0)) as server:
        client = raknet.create_connection(server.local_address, timeout=5)
        server_conn = server.accept(timeout=5)
        client.close()
        with pytest.raises(raknet.ConnectionClosedOK):
            server_conn.recv(timeout=5)
        assert not server_conn.connected


def test_poll_and_peer_state():
    with raknet.create_server((HOST, 0)) as server:
        with raknet.create_connection(server.local_address, timeout=5) as client:
            server_conn = server.accept(timeout=5)
            assert not server_conn.poll()
            client.send(b"x")
            assert server_conn.poll(timeout=5)
            assert server_conn.recv(timeout=5) == b"x"
            assert server.connections == [server_conn]
            assert server.local_address[0] == HOST


def test_recv_timeout():
    with raknet.create_server((HOST, 0)) as server:
        with raknet.create_connection(server.local_address, timeout=5) as client:
            with pytest.raises(TimeoutError):
                client.recv(timeout=0.1)
        del server
