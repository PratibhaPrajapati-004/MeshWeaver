import asyncio
import unittest

from meshweaver import UDPNode, Message, NetworkError


class MessageTests(unittest.TestCase):
    def test_message_round_trip(self):
        original = Message("TASK", "node-a", "abc123", {"x": 10})
        decoded = Message.decode(original.encode())
        self.assertEqual(decoded, original)

    def test_invalid_message_is_rejected(self):
        with self.assertRaises(NetworkError):
            Message.decode(b"not-json")


class UDPNodeTests(unittest.IsolatedAsyncioTestCase):
    async def test_two_nodes_ping_each_other(self):
        a = await UDPNode("a", logger=None).start("127.0.0.1", 0)
        b = await UDPNode("b", logger=None).start("127.0.0.1", 0)
        try:
            self.assertGreaterEqual(await a.ping(b.address), 0)
            self.assertGreaterEqual(await b.ping(a.address), 0)
        finally:
            await a.close()
            await b.close()

    async def test_custom_message_handler(self):
        received = asyncio.Event()
        result = {}

        async def handler(message, address):
            result.update(message)
            received.set()

        a = await UDPNode("a", logger=None).start("127.0.0.1", 0)
        b = await UDPNode("b", on_message=handler, logger=None).start("127.0.0.1", 0)
        try:
            await a.send_message(b.address, "HELLO", {"text": "MeshWeaver"})
            await asyncio.wait_for(received.wait(), timeout=2)
            self.assertEqual(result["payload"]["text"], "MeshWeaver")
            self.assertEqual(result["sender"], "a")
        finally:
            await a.close()
            await b.close()


if __name__ == "__main__":
    unittest.main()
