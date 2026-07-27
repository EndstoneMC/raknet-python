"""Echo server using the synchronous API.

Accepts every connection and echoes each message straight back. Run this, then
run echo_client.py against it.
"""

import argparse

import raknet


def handle(connection: raknet.Connection) -> None:
    print(f"connected: {connection.remote_address}")
    for message in connection:
        connection.send(message)
    print(f"disconnected: {connection.remote_address}")


def main(host: str, port: int) -> None:
    with raknet.create_server((host, port), max_connections=32) as server:
        print(f"listening on {server.local_address}")
        server.serve_forever(handle)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=19132)
    args = parser.parse_args()
    main(args.host, args.port)
