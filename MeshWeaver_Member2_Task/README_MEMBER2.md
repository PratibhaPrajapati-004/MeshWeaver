# MeshWeaver — Member 2: Task Serialization

This ZIP contains **only Member 2's deliverables**. It does not include or replace
Member 1's networking files.

## Files

- `net/tasks.py` — cloudpickle serialization, UDP task submission, remote execution,
  result handling, exception propagation, timeouts, and sender allow-list option.
- `tests/test_tasks.py` — tests for remote functions, closures, async functions,
  and remote exceptions.
- `requirements.txt` — Member 2's dependency.

## Integrate into the existing Member 1 GitHub repository

1. Extract this ZIP into the root of the existing MeshWeaver repository.
2. Keep Member 1's `net/network.py` and `net/__init__.py` unchanged.
3. Install the dependency:

   ```bash
   python -m pip install -r requirements.txt
   ```

4. Import Member 2 directly:

   ```python
   from net.tasks import TaskRuntime
   ```

5. Create one runtime for each already-started UDP node:

   ```python
   sender_tasks = TaskRuntime(sender_node)
   receiver_tasks = TaskRuntime(receiver_node)

   result = await sender_tasks.submit(
       receiver_node.address, your_function, arg1, arg2
   )
   print(result)

   sender_tasks.close()
   receiver_tasks.close()
   ```

`TaskRuntime` uses the existing Member 1 API:
`UDPNode.send_message(address, message_type, payload)`. It does not require
modifications to `network.py` or `net/__init__.py`.

## Run Member 2 tests

From the project root, after extracting these files:

```bash
python -m pip install -r requirements.txt
python -m unittest tests.test_tasks -v
```

If the `tests` directory is not a Python package in your repository, use:

```bash
python -m unittest discover -s tests -p "test_tasks.py" -v
```

## Security warning

`cloudpickle.loads()` can execute arbitrary Python code. Only deserialize task payloads
from mutually trusted peers. The `allowed_senders` option checks the claimed sender ID
but is not cryptographic authentication. Do not expose this executor to untrusted
networks or the public internet.
