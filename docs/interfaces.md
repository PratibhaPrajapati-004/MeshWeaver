# Interfaces

Message: {type, sender, message_id, payload}
Types: PING, PONG, TASK, RESULT
TASK payload: {"task": "<base64 of cloudpickle bytes>"}
RESULT payload: {"task_id": "...", "result": "<base64>", "error": null}

UDPNode(node_id, on_message=None, logger=None)
  await start(host, port) / await close()
  await send_message(addr, type, payload) -> message_id
  await ping(addr) -> rtt_ms
on_message(msg_dict, addr)  # keys: type, sender, message_id, payload

serialize_task(func, args) -> bytes
deserialize_task(data) -> (func, args)
