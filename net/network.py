"""Async UDP networking for MeshWeaver.

Uses only Python's standard library. Each node can both send and receive datagrams,
so there is no central server requirement.
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

MAX_DATAGRAM_SIZE = 65_507
MessageHandler = Callable[[dict[str, Any], tuple[str, int]], Any]


class NetworkError(Exception):
    """Raised for invalid messages or networking failures."""


@dataclass(frozen=True)
class Message:
    """Small JSON message envelope shared by MeshWeaver nodes."""
    message_type: str
    sender: str
    message_id: str
    payload: dict[str, Any]

    def encode(self) -> bytes:
        data = {
            "type": self.message_type,
            "sender": self.sender,
            "message_id": self.message_id,
            "payload": self.payload,
        }
        encoded = json.dumps(data, separators=(",", ":")).encode("utf-8")
        if len(encoded) > MAX_DATAGRAM_SIZE:
            raise NetworkError(
                f"Datagram is too large ({len(encoded)} bytes; "
                f"maximum is {MAX_DATAGRAM_SIZE})."
            )
        return encoded

    @classmethod
    def decode(cls, raw: bytes) -> "Message":
        try:
            data = json.loads(raw.decode("utf-8"))
            if not isinstance(data, dict):
                raise ValueError("message must be a JSON object")
            message_type = data["type"]
            sender = data["sender"]
            message_id = data["message_id"]
            payload = data.get("payload", {})
            if not all(isinstance(v, str) and v for v in
                       (message_type, sender, message_id)):
                raise ValueError("type, sender and message_id must be non-empty strings")
            if not isinstance(payload, dict):
                raise ValueError("payload must be a JSON object")
            return cls(message_type, sender, message_id, payload)
        except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            raise NetworkError(f"Invalid MeshWeaver datagram: {exc}") from exc


class _NodeProtocol(asyncio.DatagramProtocol):
    def __init__(self, node: "UDPNode") -> None:
        self.node = node

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.node._transport = transport  # type: ignore[assignment]
        sockname = transport.get_extra_info("sockname")
        self.node.address = (sockname[0], sockname[1])

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        try:
            message = Message.decode(data)
        except NetworkError as exc:
            self.node.logger(f"[{self.node.node_id}] dropped packet from {addr}: {exc}")
            return
        asyncio.create_task(self.node._handle_message(message, addr))

    def error_received(self, exc: Exception) -> None:
        self.node.logger(f"[{self.node.node_id}] UDP error: {exc}")

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.node._closed.set()


class UDPNode:
    """A peer that can send and receive messages over asynchronous UDP.

    Example:
        node = await UDPNode("node-a").start("127.0.0.1", 9001)
        await node.ping(("127.0.0.1", 9002))
        await node.close()
    """

    def __init__(
        self,
        node_id: str,
        on_message: Optional[MessageHandler] = None,
        logger: Optional[Callable[[str], None]] = print,
    ) -> None:
        if not node_id:
            raise ValueError("node_id must not be empty")
        self.node_id = node_id
        self.on_message = on_message
        self.logger = logger or (lambda _message: None)
        self.address: Optional[tuple[str, int]] = None
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._pending_pings: dict[str, asyncio.Future[float]] = {}
        self._closed = asyncio.Event()

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> "UDPNode":
        """Bind this peer to host:port. Port 0 asks the OS for a free port."""
        if self._transport is not None:
            raise NetworkError("Node is already running")
        loop = asyncio.get_running_loop()
        await loop.create_datagram_endpoint(
            lambda: _NodeProtocol(self),
            local_addr=(host, port),
        )
        self.logger(f"[{self.node_id}] listening on {self.address}")
        return self

    def _require_transport(self) -> asyncio.DatagramTransport:
        if self._transport is None or self.address is None:
            raise NetworkError("Start the node before sending messages")
        return self._transport

    async def send_message(
        self,
        address: tuple[str, int],
        message_type: str,
        payload: Optional[dict[str, Any]] = None,
    ) -> str:
        """Send one JSON datagram to another peer and return its message ID."""
        transport = self._require_transport()
        message = Message(
            message_type=message_type,
            sender=self.node_id,
            message_id=uuid.uuid4().hex,
            payload=payload or {},
        )
        transport.sendto(message.encode(), address)
        return message.message_id

    async def ping(
        self, address: tuple[str, int], timeout: float = 3.0
    ) -> float:
        """Send PING and return round-trip time in milliseconds."""
        self._require_transport()
        request_id = uuid.uuid4().hex
        future: asyncio.Future[float] = asyncio.get_running_loop().create_future()
        self._pending_pings[request_id] = future
        started = time.perf_counter()
        try:
            message = Message("PING", self.node_id, request_id, {})
            self._require_transport().sendto(message.encode(), address)
            await asyncio.wait_for(future, timeout=timeout)
            return (time.perf_counter() - started) * 1000.0
        except asyncio.TimeoutError as exc:
            raise NetworkError(f"Ping to {address} timed out after {timeout:.1f}s") from exc
        finally:
            self._pending_pings.pop(request_id, None)

    async def _handle_message(
        self, message: Message, addr: tuple[str, int]
    ) -> None:
        if message.message_type == "PING":
            # Reply directly to the sender; no coordinator/server is involved.
            pong = Message("PONG", self.node_id, message.message_id, {})
            if self._transport is not None:
                self._transport.sendto(pong.encode(), addr)
            self.logger(f"[{self.node_id}] PING received from {message.sender}; PONG sent")
            return

        if message.message_type == "PONG":
            pending = self._pending_pings.get(message.message_id)
            if pending is not None and not pending.done():
                # The sender's response is enough to complete the round trip.
                pending.set_result(time.perf_counter())
            self.logger(f"[{self.node_id}] PONG received from {message.sender}")
            return

        if self.on_message is not None:
            try:
                result = self.on_message(
                    {
                        "type": message.message_type,
                        "sender": message.sender,
                        "message_id": message.message_id,
                        "payload": message.payload,
                    },
                    addr,
                )
                if asyncio.iscoroutine(result) or isinstance(result, Awaitable):
                    await result
            except Exception as exc:
                self.logger(f"[{self.node_id}] message handler error: {exc}")

    async def close(self) -> None:
        """Close the UDP socket cleanly."""
        if self._transport is not None:
            self._transport.close()
            self._transport = None
            try:
                await asyncio.wait_for(self._closed.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
            self.logger(f"[{self.node_id}] stopped")
