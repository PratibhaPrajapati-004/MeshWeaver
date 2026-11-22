import asyncio
import unittest

from net.network import UDPNode\nfrom net.tasks import TaskRuntime, RemoteTaskError


def multiply(a, b):
    return a * b


def fail_task():
    raise ValueError("intentional failure")


def make_adder(amount):
    def add(value):
        return value + amount
    return add


class TaskRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.a = await UDPNode("node-a", logger=None).start("127.0.0.1", 0)
        self.b = await UDPNode("node-b", logger=None).start("127.0.0.1", 0)
        self.runtime_a = TaskRuntime(self.a)
        self.runtime_b = TaskRuntime(self.b)

    async def asyncTearDown(self):
        self.runtime_a.close()
        self.runtime_b.close()
        await self.a.close()
        await self.b.close()

    async def test_remote_function_and_arguments(self):
        result = await self.runtime_a.submit(self.b.address, multiply, 6, 7)
        self.assertEqual(result, 42)

    async def test_cloudpickle_closure(self):
        add_five = make_adder(5)
        self.assertEqual(await self.runtime_a.submit(self.b.address, add_five, 10), 15)

    async def test_remote_exception(self):
        with self.assertRaises(RemoteTaskError) as caught:
            await self.runtime_a.submit(self.b.address, fail_task)
        self.assertEqual(caught.exception.error_type, "ValueError")
        self.assertIn("intentional failure", str(caught.exception))

    async def test_async_function(self):
        async def async_add(a, b):
            await asyncio.sleep(0)
            return a + b
        self.assertEqual(await self.runtime_a.submit(self.b.address, async_add, 2, 3), 5)


if __name__ == "__main__":
    unittest.main()
