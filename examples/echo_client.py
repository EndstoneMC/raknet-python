"""Echo client using the synchronous API.

Connects to echo_server.py, sends a few messages and prints what comes back.

RakNet delivers only packets whose first byte is a user message id (>= 0x86);
Minecraft Bedrock leads with 0xFE. Every payload must start with such a byte, so
these examples prepend PACKET_ID.
"""

import argparse

import raknet

PACKET_ID = b"\xfe"


def main(host: str, port: int) -> None:
    with raknet.create_connection((host, port), timeout=5) as connection:
        print(f"connected to {connection.remote_address}")
        for line in ("hello", "raknet", "goodbye"):
            connection.send(PACKET_ID + line.encode())
            reply = connection.recv(timeout=5)
            print(f"echoed: {reply[1:].decode()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19132)
    args = parser.parse_args()
    main(args.host, args.port)
