"""A scripted stand-in for `claude -p --input-format stream-json`, for the input-pump tests.

Speaks claude's stream-json on both pipes, following the measured behaviour (see
tests/test_live_input_real.py): one `result` per turn, a message arriving mid-turn folded into that
turn, background tasks reported with task_started/task_updated/task_notification and followed by a
turn of their own, EOF ending the process (killing any background task first).

Each user message's text is a command:

    reply <text>       answer with <text>
    slow <secs> <text> work for <secs>, folding in anything that arrives meanwhile, then answer
    tool <secs> <text> emit a foreground Bash tool call, wait, emit its tool result, then answer
    cap              emit error_max_turns once, then success on the next invocation
    bg <id> <secs>     start a background task that finishes after <secs>, answer STARTED
    silentbg <id>      start a background task that never finishes, answer STARTED
    error              end the turn with an error result
    die <code>         exit at once with <code>, no result

Every line received on stdin is appended to $FAKE_CLAUDE_LOG, one JSON object per line. The first
argument is the session id it reports.
"""

from __future__ import annotations

import json
import os
import queue
import sys
import threading
import time

SESSION = sys.argv[1] if len(sys.argv) > 1 else "fake-session"
LOG = os.environ.get("FAKE_CLAUDE_LOG")
_out = threading.Lock()
_inbox: queue.Queue = queue.Queue()
_cost = [0.0]


def emit(event: dict) -> None:
    event.setdefault("session_id", SESSION)
    with _out:
        sys.stdout.write(json.dumps(event) + "\n")
        sys.stdout.flush()


def result(text: str, *, error: bool = False, subtype: str | None = None) -> None:
    _cost[0] += 0.01
    emit(
        {
            "type": "result",
            "subtype": subtype or ("error_during_execution" if error else "success"),
            "is_error": error,
            "result": text,
            "num_turns": 1,
            "total_cost_usd": round(_cost[0], 4),
            "usage": {"input_tokens": 1, "output_tokens": 1},
            "permission_denials": [],
        }
    )


def assistant(text: str) -> None:
    emit({"type": "assistant", "message": {"content": [{"type": "text", "text": text}]}})


def _reader() -> None:
    for line in sys.stdin:
        if LOG:
            with open(LOG, "a") as handle:
                handle.write(line if line.endswith("\n") else line + "\n")
        try:
            message = json.loads(line)
            text = message["message"]["content"][0]["text"]
        except (ValueError, KeyError, IndexError, TypeError):
            continue
        _inbox.put(("user", text))
    _inbox.put(("eof", None))


def _finish_background(task_id: str, seconds: float) -> None:
    time.sleep(seconds)
    if task_id not in open_background:
        return
    open_background.discard(task_id)
    emit({"type": "system", "subtype": "task_updated", "task_id": task_id, "patch": {"status": "completed"}})
    emit({"type": "system", "subtype": "task_notification", "task_id": task_id, "status": "completed"})
    _inbox.put(("bgdone", task_id))


open_background: set[str] = set()


def start_background(task_id: str) -> None:
    open_background.add(task_id)
    emit(
        {
            "type": "system",
            "subtype": "task_started",
            "task_id": task_id,
            "is_backgrounded": True,
            "task_type": "local_bash",
        }
    )


def turn(text: str, pending_eof: list[bool]) -> None:
    words = text.split()
    command = words[0] if words else ""
    if command == "reply":
        assistant(" ".join(words[1:]))
        result(" ".join(words[1:]))
    elif command == "slow":
        seconds, answer = float(words[1]), " ".join(words[2:])
        assistant("working")
        folded: list[str] = []
        deadline = time.monotonic() + seconds
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                kind, value = _inbox.get(timeout=remaining)
            except queue.Empty:
                break
            if kind == "user":
                folded.append(value)
            elif kind == "eof":
                pending_eof[0] = True
            else:
                _inbox.put((kind, value))
        result(answer + ("" if not folded else " | folded: " + " / ".join(folded)))
    elif command == "tool":
        seconds, answer = float(words[1]), " ".join(words[2:])
        emit({"type": "assistant", "message": {"content": [{"type": "tool_use", "id": "toolu_foreground", "name": "Bash", "input": {"command": "sleep"}}]}})
        folded: list[str] = []
        deadline = time.monotonic() + seconds
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                kind, value = _inbox.get(timeout=remaining)
            except queue.Empty:
                break
            if kind == "user":
                folded.append(value)
            elif kind == "eof":
                pending_eof[0] = True
            else:
                _inbox.put((kind, value))
        emit({"type": "user", "message": {"content": [{"type": "tool_result", "tool_use_id": "toolu_foreground", "content": "done"}]}})
        result(answer + ("" if not folded else " | folded: " + " / ".join(folded)))
    elif command == "cap":
        counter = os.environ.get("FAKE_CLAUDE_CAP_COUNTER")
        count = 0
        if counter:
            try:
                count = int(open(counter).read())
            except (FileNotFoundError, ValueError):
                pass
            with open(counter, "w") as handle:
                handle.write(str(count + 1))
        ready = os.environ.get("FAKE_CLAUDE_CAP_READY")
        if ready:
            open(ready, "a").close()
        time.sleep(float(os.environ.get("FAKE_CLAUDE_CAP_DELAY", "0")))
        result(
            "cutoff" if count == 0 else "success",
            error=count == 0,
            subtype="error_max_turns" if count == 0 else None,
        )
        return
    elif command == "bg":
        start_background(words[1])
        threading.Thread(
            target=_finish_background, args=(words[1], float(words[2])), daemon=True
        ).start()
        assistant("STARTED")
        result("STARTED")
    elif command == "silentbg":
        start_background(words[1])
        assistant("STARTED")
        result("STARTED")
    elif command == "error":
        result("it broke", error=True)
    elif command == "die":
        sys.stdout.flush()
        os._exit(int(words[1]) if len(words) > 1 else 1)
    else:
        assistant(text)
        result(text)


def main() -> None:
    emit({"type": "system", "subtype": "init", "tools": [], "mcp_servers": []})
    threading.Thread(target=_reader, daemon=True).start()
    pending_eof = [False]
    while True:
        if pending_eof[0] and _inbox.empty():
            break
        kind, value = _inbox.get()
        if kind == "eof":
            if _inbox.empty():
                break
            pending_eof[0] = True
            continue
        if kind == "bgdone":
            assistant(f"background {value} finished")
            result(f"bg {value} done")
            continue
        turn(value, pending_eof)
    for task_id in sorted(open_background):
        emit({"type": "system", "subtype": "task_updated", "task_id": task_id, "patch": {"status": "killed"}})
        emit({"type": "system", "subtype": "task_notification", "task_id": task_id, "status": "stopped"})
    sys.exit(0)


if __name__ == "__main__":
    main()
