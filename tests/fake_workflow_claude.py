"""Small Claude CLI stand-in for the detached workflow recovery acceptance test."""

from __future__ import annotations

import json
import os
import re
import sys
import time


def _prompt() -> str:
    args = sys.argv[1:]
    return args[-1] if args else sys.stdin.read()


def _context(prompt: str) -> dict | None:
    match = re.search(r"Context:\n(\{.*\})\s*$", prompt, re.DOTALL)
    if match:
        try:
            value = json.loads(match.group(1))
        except json.JSONDecodeError:
            return None
        return value if isinstance(value, dict) else None
    try:
        value, _ = json.JSONDecoder().raw_decode(prompt)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(value, dict) or "checkpoint" not in value:
        return None
    bootstrap = value.get("bootstrap", {}).get("context", {})
    checkpoint = value.get("checkpoint", {})
    return {**bootstrap, **checkpoint} if isinstance(bootstrap, dict) and isinstance(checkpoint, dict) else None


def _emit(event: dict) -> None:
    print(json.dumps(event), flush=True)


def main() -> None:
    prompt = _prompt()
    session = os.environ.get("HARFLOW_FAKE_WORKFLOW_SESSION", "workflow-session")
    _emit({"type": "system", "subtype": "init", "session_id": session})

    if "Assignment:\n" in prompt:
        ready = os.environ["HARFLOW_WORKFLOW_WORKER_READY"]
        with open(ready, "w", encoding="utf-8") as handle:
            json.dump({"pid": os.getpid(), "session_id": session}, handle)
        release = os.environ["HARFLOW_WORKFLOW_WORKER_RELEASE"]
        deadline = time.monotonic() + 15
        while not os.path.exists(release) and time.monotonic() < deadline:
            time.sleep(0.02)
        output = json.dumps({"status": "succeeded", "result": {"summary": "Done"}, "evidence": ["observed"]})
    else:
        context = _context(prompt) or {}
        choices = context.get("valid_continuations", [])
        if choices:
            next_entries = []
            for choice in choices:
                entry = {"continuation_id": choice["continuation_id"]}
                if choice.get("requires_prompt"):
                    entry.update(prompt="Complete the assigned work", session_mode="fresh")
                next_entries.append(entry)
            decision = {"decision_id": context.get("decision_id"), "action": "continue", "reason": "Continue", "next": next_entries}
        else:
            decision = {"decision_id": context.get("decision_id"), "action": "complete", "reason": "Complete", "next": []}
        output = json.dumps(decision)

    _emit({"type": "assistant", "message": {"content": [{"type": "text", "text": output}]}, "session_id": session})
    _emit({"type": "result", "subtype": "success", "is_error": False, "result": output, "session_id": session, "num_turns": 1, "total_cost_usd": 0, "usage": {"input_tokens": 1, "output_tokens": 1}, "permission_denials": []})
    # Leave the pipes open briefly so the detached task owner can drain the
    # terminal result even while the workflow scheduler is being recovered.
    time.sleep(0.5)


if __name__ == "__main__":
    main()
