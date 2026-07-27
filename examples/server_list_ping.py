"""Query a Bedrock server's MOTD with an unconnected ping.

Starts a throwaway server advertising an offline ping response, then pings it and
prints the reply. Point --host/--port at a real server to query it instead.
"""

import argparse
import struct

import raknet

MOTD = "MCPE;Dedicated Server;390;1.14.60;0;10;13253860892328930865;Bedrock level;Survival;1;19132;19133;"


def main(host: str, port: int, serve: bool) -> None:
    server = None
    if serve:
        response = struct.pack(">H", len(MOTD)) + MOTD.encode()
        server = raknet.create_server((host, port), offline_ping_response=response)

    try:
        pong = raknet.ping((host, port), timeout=5)
        print(f"pong from {pong.address}")
        print(f"guid: {pong.guid}")
        print(f"round trip: {pong.round_trip_time * 1000:.1f} ms")
        print(f"motd: {pong.data.decode(errors='replace')}")
    finally:
        if server is not None:
            server.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19132)
    parser.add_argument("--no-serve", dest="serve", action="store_false", help="ping an existing server")
    args = parser.parse_args()
    main(args.host, args.port, args.serve)
