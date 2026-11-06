"""Audited local protocol subprocess: barriers only, no provider calls or sleeps."""
import json
import socket
import sys

with socket.socket(socket.AF_UNIX) as channel:
    channel.connect(sys.argv[1])
    stream = channel.makefile("rwb", buffering=0)
    request = json.loads(sys.argv[2])
    stream.write((json.dumps(request) + "\n").encode())
    for line in stream:
        command = json.loads(line)
        if command.get("exit"):
            break
        print(json.dumps({"type": "result", "subtype": "success", "is_error": False,
            "result": command["result"], "session_id": request["session_id"],
            "num_turns": 1}), flush=True)
        stream.write(b"result_written\n")
