import json, uuid, time

MSG_TYPES = {"PING", "PONG", "TASK", "RESULT"}

def make_message(msg_type, sender_id, payload=None):
    return {
        "type": msg_type,
        "sender_id": sender_id,
        "payload": payload or {},
        "id": str(uuid.uuid4()),
        "ts": time.time(),
    }

def encode(msg) -> bytes:
    return json.dumps(msg).encode()

def decode(data: bytes) -> dict:
    msg = json.loads(data.decode())
    if msg.get("type") not in MSG_TYPES:
        raise ValueError(f"Unknown message type: {msg.get('type')}")
    return msg