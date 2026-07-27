import asyncio

import pytest

import raknet
import raknet.asyncio

HOST = "127.0.0.1"


def test_echo_roundtrip():
    async def main():
        async with await raknet.asyncio.create_server((HOST, 0), max_connections=4) as server:

            async def serve():
                conn = await server.accept()
                async for message in conn:
                    await conn.send(message)

            task = asyncio.create_task(serve())
            conn = await raknet.asyncio.create_connection(server.local_address)
            async with conn:
                assert conn.connected
                assert conn.remote_address == server.local_address
                await conn.send(b"hello asyncio")
                assert await asyncio.wait_for(conn.recv(), 5) == b"hello asyncio"
            await asyncio.wait_for(task, 5)

    asyncio.run(asyncio.wait_for(main(), 30))


def test_serve_forever_echo():
    async def main():
        async with await raknet.asyncio.create_server((HOST, 0), max_connections=4) as server:

            async def handler(conn):
                async for message in conn:
                    await conn.send(message)

            serve_task = asyncio.create_task(server.serve_forever(handler))
            for payload in (b"first", b"second"):
                conn = await raknet.asyncio.create_connection(server.local_address)
                async with conn:
                    await conn.send(payload)
                    assert await asyncio.wait_for(conn.recv(), 5) == payload
        await asyncio.wait_for(serve_task, 5)

    asyncio.run(asyncio.wait_for(main(), 30))


def test_serve_forever_closes_connection_after_handler():
    async def main():
        async with await raknet.asyncio.create_server((HOST, 0)) as server:

            async def handler(conn):
                pass

            serve_task = asyncio.create_task(server.serve_forever(handler))
            conn = await raknet.asyncio.create_connection(server.local_address)
            async with conn:
                with pytest.raises(raknet.ConnectionClosedOK):
                    await asyncio.wait_for(conn.recv(), 5)
        await asyncio.wait_for(serve_task, 5)

    asyncio.run(asyncio.wait_for(main(), 30))


def test_offline_ping():
    async def main():
        async with await raknet.asyncio.create_server((HOST, 0), offline_ping_response=b"MOTD") as server:
            pong = await asyncio.wait_for(raknet.asyncio.ping(server.local_address), 5)
            assert pong.data == b"MOTD"
            assert pong.guid == server.guid
            assert 0 <= pong.round_trip_time < 5

    asyncio.run(asyncio.wait_for(main(), 30))


def test_invalid_password():
    async def main():
        async with await raknet.asyncio.create_server((HOST, 0), password=b"sekret") as server:
            with pytest.raises(raknet.ConnectError) as excinfo:
                await asyncio.wait_for(raknet.asyncio.create_connection(server.local_address, password=b"wrong"), 5)
            assert excinfo.value.reason == raknet.raw.DefaultMessageIDTypes.ID_INVALID_PASSWORD

    asyncio.run(asyncio.wait_for(main(), 30))


def test_close_notification():
    async def main():
        async with await raknet.asyncio.create_server((HOST, 0)) as server:
            client = await raknet.asyncio.create_connection(server.local_address)
            server_conn = await asyncio.wait_for(server.accept(), 5)
            await client.close()
            with pytest.raises(raknet.ConnectionClosedOK):
                await asyncio.wait_for(server_conn.recv(), 5)
            assert not server_conn.connected

    asyncio.run(asyncio.wait_for(main(), 30))


def test_recv_cancellation_via_timeout():
    async def main():
        async with await raknet.asyncio.create_server((HOST, 0)) as server:
            client = await raknet.asyncio.create_connection(server.local_address)
            async with client:
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(client.recv(), 0.1)
        del server

    asyncio.run(asyncio.wait_for(main(), 30))


def test_concurrent_clients():
    async def main():
        async with await raknet.asyncio.create_server((HOST, 0), max_connections=8) as server:

            async def serve():
                conns = []
                for _ in range(3):
                    conns.append(await server.accept())
                for conn in conns:
                    message = await conn.recv()
                    await conn.send(message + b"-ack")

            task = asyncio.create_task(serve())
            clients = await asyncio.gather(*(raknet.asyncio.create_connection(server.local_address) for _ in range(3)))
            for i, client in enumerate(clients):
                await client.send(b"c%d" % i)
            for i, client in enumerate(clients):
                assert await asyncio.wait_for(client.recv(), 5) == b"c%d-ack" % i
            await asyncio.wait_for(task, 5)
            for client in clients:
                await client.close()

    asyncio.run(asyncio.wait_for(main(), 30))
