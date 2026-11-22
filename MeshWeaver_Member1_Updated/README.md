# MeshWeaver — Member 1: Network Development

This module implements the Week 1 networking work for **MeshWeaver: Zero-Dependency P2P Async Task Broker**.

## Project structure

```text
MeshWeaver/
├── net/
│   ├── __init__.py
│   └── network.py
├── tests/
│   └── test_network.py
├── run_demo.py
└── README.md
```

## Completed responsibilities

- Asynchronous UDP peer using Python `asyncio`.
- UDP send/client functionality through `UDPNode.send_message()` and `UDPNode.ping()`.
- Two independent Python processes can communicate directly.
- Basic PING/PONG request-response mechanism with timeout.
- JSON message envelope with sender ID, message ID, type, and payload.
- Generic incoming-message callback for integration with the task/protocol modules.
- Unit tests for message serialization, invalid packets, custom messages, and bidirectional ping.

## Requirements

Python 3.10 or newer. No third-party packages are required.

## Run two peers in separate terminals

Open two terminals in the project root.

Terminal 1:

```bash
python run_demo.py --port 5000
```

Terminal 2:

```bash
python run_demo.py --port 5001
```

By default, port 5000 targets 5001 and port 5001 targets 5000. Both processes remain active and periodically send PING messages. Stop either peer with `Ctrl+C`.

You can override the remote peer explicitly:

```bash
python run_demo.py --port 5002 --peer-port 5003
```

For communication between different machines, set `--host` and `--peer-host` to reachable interface/IP addresses and allow the UDP ports through the firewall.

## Run tests

From the project root:

```bash
python -m unittest discover -s tests -v
```

## Use the networking module

```python
import asyncio
from net import UDPNode

async def main():
    peer_a = await UDPNode("peer-a").start("127.0.0.1", 9001)
    peer_b = await UDPNode("peer-b").start("127.0.0.1", 9002)
    try:
        latency_ms = await peer_a.ping(peer_b.address)
        print(f"Peer B responded in {latency_ms:.2f} ms")
    finally:
        await peer_a.close()
        await peer_b.close()

asyncio.run(main())
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
