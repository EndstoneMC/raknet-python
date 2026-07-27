"""Echo client using the asyncio API.

Connects to async_echo_server.py, sends a few messages and prints the replies.
See echo_client.py for why every payload starts with PACKET_ID.
"""

import argparse
import asyncio

import raknet.asyncio

PACKET_ID = b"\xfe"


async def main(host: str, port: int) -> None:
    connection = await raknet.asyncio.create_connection((host, port))
    async with connection:
        print(f"connected to {connection.remote_address}")
        for line in ("hello", "raknet", "goodbye"):
            await connection.send(PACKET_ID + line.encode())
            reply = await asyncio.wait_for(connection.recv(), timeout=5)
            print(f"echoed: {reply[1:].decode()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19132)
    args = parser.parse_args()
    asyncio.run(main(args.host, args.port))
