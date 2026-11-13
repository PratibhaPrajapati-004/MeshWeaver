"""Run two independent MeshWeaver peers and test bidirectional UDP ping/pong."""
import asyncio
from meshweaver import UDPNode


async def main():
    node_a = await UDPNode("node-A").start("127.0.0.1", 0)
    node_b = await UDPNode("node-B").start("127.0.0.1", 0)

    try:
        print("\n--- Node A pings Node B ---")
        rtt_ab = await node_a.ping(node_b.address)
        print(f"Node A -> Node B: {rtt_ab:.2f} ms")

        print("\n--- Node B pings Node A ---")
        rtt_ba = await node_b.ping(node_a.address)
        print(f"Node B -> Node A: {rtt_ba:.2f} ms")

        print("\nSUCCESS: both peers communicated without a central server.")
    finally:
        await node_a.close()
        await node_b.close()


if __name__ == "__main__":
    asyncio.run(main())
