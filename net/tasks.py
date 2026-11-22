"""Cloudpickle-based remote task submission over MeshWeaver UDP.

IMPORTANT: cloudpickle payloads are executable Python serialization. Only use
this module between mutually trusted peers on a trusted network.
"""
from __future__ import annotations

import asyncio
import base64
import binascii
import inspect
import traceback
import uuid
from typing import Any, Callable, Optional

try:
    import cloudpickle
except ImportError as exc:  # pragma: no cover - exercised in missing-dependency installs
    raise ImportError(
        "MeshWeaver task serialization requires cloudpickle. "
        "Install dependencies with: pip install -r requirements.txt"
    ) from exc

from .network import NetworkError, UDPNode

TASK = "TASK"
TASK_RESULT = "TASK_RESULT"


class TaskError(Exception):
    """Base exception for remote task failures."""


class RemoteTaskError(TaskError):
    """A remote function raised an exception."""

    def __init__(self, error_type: str, error: str, remote_traceback: str = ""):
        self.error_type = error_type
        self.error = error
        self.remote_traceback = remote_traceback
        detail = f"Remote {error_type}: {error}"
        if remote_traceback:
            detail += f"\\nRemote traceback:\\n{remote_traceback}"
        super().__init__(detail)


class TaskRuntime:
    """Adds request/response task execution to an existing UDPNode.

    Create one runtime per node. The node remains responsible for UDP transport.
    """

    def __init__(
        self,
        node: UDPNode,
        *,
        max_task_bytes: int = 48_000,
        allowed_senders: Optional[set[str]] = None,
        offload_sync: bool = True,
    ) -> None:
        if max_task_bytes <= 0 or max_task_bytes > 48_000:
            raise ValueError("max_task_bytes must be between 1 and 48000")
        self.node = node
        self.max_task_bytes = max_task_bytes
        self.allowed_senders = allowed_senders
        self.offload_sync = offload_sync
        self._pending: dict[str, asyncio.Future[Any]] = {}
        self._previous_handler = node.on_message
        node.on_message = self._handle_message

    async def submit(
        self,
        address: tuple[str, int],
        function: Callable[..., Any],
        *args: Any,
        timeout: float = 30.0,
        **kwargs: Any,
    ) -> Any:
        """Serialize and execute a callable remotely, returning its result."""
        if not callable(function):
            raise TypeError("function must be callable")
        if timeout <= 0:
            raise ValueError("timeout must be positive")

        try:
            serialized = cloudpickle.dumps((function, args, kwargs), protocol=5)
        except Exception as exc:
            raise TaskError(f"Could not serialize task: {exc}") from exc
        if len(serialized) > self.max_task_bytes:
            raise NetworkError(
                f"Serialized task is {len(serialized)} bytes; "
                f"configured maximum is {self.max_task_bytes}. "
                "Use smaller tasks or implement chunked/stream transport."
            )

        payload = {"data": base64.b64encode(serialized).decode("ascii")}
        task_id = await self.node.send_message(address, TASK, payload)
        future = asyncio.get_running_loop().create_future()
        self._pending[task_id] = future
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise TaskError(f"Remote task {task_id} timed out after {timeout}s") from exc
        finally:
            self._pending.pop(task_id, None)

    async def _handle_message(
        self, message: dict[str, Any], address: tuple[str, int]
    ) -> None:
        message_type = message.get("type")
        if message_type == TASK:
            await self._execute_task(message, address)
            return
        if message_type == TASK_RESULT:
            self._receive_result(message)
            return
        if self._previous_handler is not None:
            result = self._previous_handler(message, address)
            if inspect.isawaitable(result):
                await result

    async def _execute_task(
        self, message: dict[str, Any], address: tuple[str, int]
    ) -> None:
        sender = message.get("sender", "")
        task_id = message.get("message_id", "")
        payload = message.get("payload", {})
        if self.allowed_senders is not None and sender not in self.allowed_senders:
            await self._send_error(address, task_id, "PermissionError",
                                   f"sender {sender!r} is not allowed", "")
            return

        try:
            encoded = payload["data"]
            if not isinstance(encoded, str):
                raise ValueError("task data must be base64 text")
            raw = base64.b64decode(encoded, validate=True)
            if len(raw) > self.max_task_bytes:
                raise ValueError("task exceeds configured size limit")
            # SECURITY: unpickling can execute arbitrary code. Trust the sender.
            function, args, kwargs = cloudpickle.loads(raw)
            if not callable(function) or not isinstance(args, tuple) or not isinstance(kwargs, dict):
                raise ValueError("invalid serialized task structure")

            if inspect.iscoroutinefunction(function):
                result = await function(*args, **kwargs)
            elif self.offload_sync:
                result = await asyncio.to_thread(function, *args, **kwargs)
            else:
                result = function(*args, **kwargs)

            result_data = cloudpickle.dumps({"ok": True, "result": result}, protocol=5)
            await self._send_result(address, task_id, result_data)
        except Exception as exc:
            await self._send_error(
                address, task_id, type(exc).__name__, str(exc),
                traceback.format_exc(),
            )

    async def _send_result(
        self, address: tuple[str, int], task_id: str, data: bytes
    ) -> None:
        if len(data) > self.max_task_bytes:
            await self._send_error(
                address, task_id, "ResultTooLarge",
                f"serialized result exceeds {self.max_task_bytes} bytes", "",
            )
            return
        await self.node.send_message(
            address,
            TASK_RESULT,
            {"task_id": task_id, "data": base64.b64encode(data).decode("ascii")},
        )

    async def _send_error(
        self, address: tuple[str, int], task_id: str, error_type: str,
        error: str, remote_traceback: str,
    ) -> None:
        body = cloudpickle.dumps({
            "ok": False, "error_type": error_type, "error": error,
            "traceback": remote_traceback,
        }, protocol=5)
        await self._send_result(address, task_id, body)

    def _receive_result(self, message: dict[str, Any]) -> None:
        task_id = message.get("payload", {}).get("task_id")
        future = self._pending.get(task_id)
        if future is None or future.done():
            return
        try:
            encoded = message["payload"]["data"]
            raw = base64.b64decode(encoded, validate=True)
            if len(raw) > self.max_task_bytes:
                raise ValueError("result exceeds configured size limit")
            response = cloudpickle.loads(raw)
            if not isinstance(response, dict) or "ok" not in response:
                raise ValueError("invalid task response")
            if response["ok"]:
                future.set_result(response.get("result"))
            else:
                future.set_exception(RemoteTaskError(
                    response.get("error_type", "RemoteError"),
                    response.get("error", "unknown error"),
                    response.get("traceback", ""),
                ))
        except (KeyError, TypeError, ValueError, binascii.Error, Exception) as exc:
            if not future.done():
                future.set_exception(TaskError(f"Invalid task result: {exc}"))

    def close(self) -> None:
        """Detach this runtime and cancel outstanding submissions."""
        if self.node.on_message == self._handle_message:
            self.node.on_message = self._previous_handler
        for future in self._pending.values():
            if not future.done():
                future.cancel()
        self._pending.clear()
