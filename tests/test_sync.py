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


def test_serve_forever_echo():
    with raknet.create_server((HOST, 0), max_connections=4) as server:

        def handler(conn):
            for message in conn:
                conn.send(message)

        thread = threading.Thread(target=server.serve_forever, args=(handler,), daemon=True)
        thread.start()
        for payload in (b"first", b"second"):
            with raknet.create_connection(server.local_address, timeout=5) as conn:
                conn.send(payload)
                assert conn.recv(timeout=5) == payload
    thread.join(timeout=5)
    assert not thread.is_alive()


def test_serve_forever_closes_connection_after_handler():
    with raknet.create_server((HOST, 0)) as server:
        thread = threading.Thread(target=server.serve_forever, args=(lambda conn: None,), daemon=True)
        thread.start()
        with raknet.create_connection(server.local_address, timeout=5) as conn:
            with pytest.raises(raknet.ConnectionClosedOK):
                conn.recv(timeout=5)
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


def test_connect_unresolvable_host():
    with raknet.create_server((HOST, 0)) as server:
        with pytest.raises(raknet.ConnectError) as excinfo:
            server.connect(("no.such.host.invalid", 19132), timeout=5)
        assert excinfo.value.reason == raknet.raw.ConnectionAttemptResult.CANNOT_RESOLVE_DOMAIN_NAME


def test_concurrent_connects_to_distinct_servers():
    with raknet.create_server((HOST, 0)) as a, raknet.create_server((HOST, 0)) as b:
        for server in (a, b):
            threading.Thread(target=server.serve_forever, args=(lambda c: [c.send(m) for m in c],), daemon=True).start()
        with raknet.create_server((HOST, 0), max_connections=4) as client:
            results = {}

            def go(target, tag):
                conn = client.connect(target.local_address, timeout=5)
                conn.send(tag)
                results[tag] = conn.recv(timeout=5)

            threads = [
                threading.Thread(target=go, args=(a, b"aaa")),
                threading.Thread(target=go, args=(b, b"bbb")),
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=10)
            assert results == {b"aaa": b"aaa", b"bbb": b"bbb"}


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


def test_max_queue_overflow_closes_connection():
    with raknet.create_server((HOST, 0), max_queue=2) as server:
        with raknet.create_connection(server.local_address, timeout=5) as client:
            server_conn = server.accept(timeout=5)
            for i in range(20):
                client.send(b"m%d" % i, reliability=raknet.PacketReliability.RELIABLE_ORDERED)
            with pytest.raises(raknet.ConnectionClosedError):
                for _ in range(20):
                    server_conn.recv(timeout=5)
            assert not server_conn.connected


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_pump_crash_tears_down_peer():
    with raknet.create_server((HOST, 0)) as server:
        client = raknet.create_connection(server.local_address, timeout=5)
        server_conn = server.accept(timeout=5)

        def boom(packet):
            raise RuntimeError("boom")

        server._router.route = boom
        client.send(b"trigger")
        with pytest.raises(raknet.ConnectionClosedError):
            server_conn.recv(timeout=5)
        assert not server_conn.connected
        client.close()
