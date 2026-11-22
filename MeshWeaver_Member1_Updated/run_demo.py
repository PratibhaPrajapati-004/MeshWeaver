"""Run one MeshWeaver UDP peer; launch a second terminal with the other port."""
import argparse
import asyncio

from net.network import UDPNode


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run a MeshWeaver UDP peer.")
    parser.add_argument("--host", default="127.0.0.1", help="Local bind host")
    parser.add_argument("--port", type=int, default=5000, help="Local UDP port")
    parser.add_argument(
        "--peer-host", default="127.0.0.1", help="Remote peer host"
    )
    parser.add_argument(
        "--peer-port",
        type=int,
        help="Remote peer UDP port (defaults to the other demo port: 5000/5001)",
    )
    args = parser.parse_args()

    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")

    peer_port = args.peer_port
    if peer_port is None:
        peer_port = 5001 if args.port == 5000 else 5000
    if not 1 <= peer_port <= 65535:
        parser.error("--peer-port must be between 1 and 65535")

    node = await UDPNode(f"node-{args.port}").start(args.host, args.port)
    peer_address = (args.peer_host, peer_port)
    print(f"Peer listening on {args.host}:{args.port}")
    print(f"Peer target is {args.peer_host}:{peer_port}")
    print("Press Ctrl+C to stop. Start the other peer in a second terminal.")

    try:
        while True:
            try:
                rtt = await node.ping(peer_address, timeout=2.0)
                print(f"PING/PONG {args.port} -> {peer_port}: {rtt:.2f} ms")
            except Exception as exc:
                print(f"Peer {peer_port} not reachable yet: {exc}")
            await asyncio.sleep(2)
    finally:
        await node.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nMeshWeaver peer stopped.")
