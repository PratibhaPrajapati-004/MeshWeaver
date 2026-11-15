import pytest
from protocol.message import make_message, encode, decode

def test_roundtrip():
    msg = make_message("PING", "node1")
    assert decode(encode(msg))["type"] == "PING"

def test_bad_type():
    with pytest.raises(ValueError):
        decode(b'{"type": "FOO", "sender_id": "x", "payload": {}}')