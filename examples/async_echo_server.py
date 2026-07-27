"""Echo server using the asyncio API.

The asyncio flavour mirrors the synchronous one with `await` in front. Run this,
then run async_echo_client.py against it.
"""

import argparse
import asyncio

import raknet.asyncio


async def handle(connection: raknet.asyncio.Connection) -> None:
    print(f"connected: {connection.remote_address}")
    async for message in connection:
        await connection.send(message)
    print(f"disconnected: {connection.remote_address}")


async def main(host: str, port: int) -> None:
    server = await raknet.asyncio.create_server((host, port), max_connections=32)
    async with server:
        print(f"listening on {server.local_address}")
        await server.serve_forever(handle)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=19132)
    args = parser.parse_args()
    asyncio.run(main(args.host, args.port))
