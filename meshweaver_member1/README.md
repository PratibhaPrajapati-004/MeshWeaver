# MeshWeaver — Member 1: Network Development

This module implements the Week 1 networking work for **MeshWeaver: Zero-Dependency P2P Async Task Broker**.

## Completed responsibilities

- Asynchronous UDP peer using Python `asyncio`.
- UDP send/client functionality through `UDPNode.send_message()` and `UDPNode.ping()`.
- Two independent Python node instances can communicate directly.
- Basic PING/PONG request-response mechanism with timeout.
- JSON message envelope with sender ID, message ID, type, and payload.
- Generic incoming-message callback for integration with the task/protocol modules.
- Unit tests for message serialization, invalid packets, custom messages, and bidirectional ping.

## Requirements

Python 3.10 or newer. **No third-party packages are required.**

## Run the two-node demonstration

From this directory:

```bash
python run_demo.py
```

The demo starts two peers on separate OS-assigned UDP ports, then tests A → B and B → A. Each peer can send and receive; neither is a central server.

## Run tests

```bash
python -m unittest discover -s tests -v
```

## Use the networking module

```python
import asyncio
from meshweaver import UDPNode

async def main():
    peer_a = await UDPNode("peer-a").start("127.0.0.1", 9001)
    peer_b = await UDPNode("peer-b").start("127.0.0.1", 9002)

    try:
        latency_ms = await peer_a.ping(peer_b.address)
        print(f"Peer B responded in {latency_ms:.2f} ms")
        await peer_a.send_message(peer_b.address, "HELLO", {"text": "Hi"})
    finally:
        await peer_a.close()
        await peer_b.close()

asyncio.run(main())
```

To receive application messages, pass an `on_message` callback:

```python
def handle_message(message, sender_address):
    print(message["type"], message["payload"], "from", message["sender"])

node = await UDPNode("peer-c", on_message=handle_message).start("0.0.0.0", 9003)
```

## Message format

Each UDP datagram contains one UTF-8 JSON object:

```json
{
  "type": "PING",
  "sender": "peer-a",
  "message_id": "unique-id",
  "payload": {}
}
```

`PING` is answered automatically with `PONG` using the same `message_id`. Other message types are passed to the configured callback.

## Integration notes for the team

- Member 2 can pass serialized task bytes in a future payload design. UDP has a strict datagram-size limit; this implementation rejects oversized JSON datagrams rather than silently truncating them. Large tasks should later use chunking or a stream transport.
- Member 3 can standardize message types and payload fields. The current envelope (`type`, `sender`, `message_id`, `payload`) is intentionally small and easy to extend.
- This is a Week 1 transport foundation, not yet a production-secure task executor. UDP is best-effort: packets can be lost, duplicated, or reordered. Authentication, retries, encryption, task validation, and safe execution must be designed before running untrusted tasks.
