import json, uuid, time

MSG_TYPES = {"PING", "PONG", "TASK", "RESULT"}

def make_message(msg_type, sender, payload=None):
    return {
        "type": msg_type,
        "sender": sender,
        "payload": payload or {},
        "message_id": str(uuid.uuid4()),
    }

def encode(msg) -> bytes:
    return json.dumps(msg).encode()

def decode(data: bytes) -> dict:
    msg = json.loads(data.decode())
    if msg.get("type") not in MSG_TYPES:
        raise ValueError(f"Unknown message type: {msg.get('type')}")
    return msg