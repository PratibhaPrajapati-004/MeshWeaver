# Interfaces
Message: {type, sender_id, payload, id, ts}
Types: PING, PONG, TASK, RESULT
TASK payload: {"task": "<base64 of cloudpickle bytes>"}
RESULT payload: {"task_id": "...", "result": "<base64>", "error": null}
send_message(addr, msg)
serialize_task(func, args)
