"""Short-lived immutable transport pages for the local Monitor CLI.

This cache never updates workflow records and is not a managed-agent read API.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import re
import time
import uuid

CHUNK_SIZE = 128 * 1024
TTL = 300


def snapshot(run: dict | None, run_id: str, cache: Path, cursor: str | None = None) -> dict:
    cache.mkdir(mode=0o700, parents=True, exist_ok=True)
    now = time.time()
    for path in cache.glob("*.json"):
        try:
            if now - path.stat().st_mtime > TTL:
                path.unlink(missing_ok=True)
        except FileNotFoundError:
            pass
    offset = 0
    if cursor:
        try:
            decoded = json.loads(base64.urlsafe_b64decode(cursor))
            token, offset, digest, owner = decoded["snapshot"], decoded["offset"], decoded["digest"], decoded["run"]
            if not isinstance(token, str) or not re.fullmatch(r"[0-9a-f]{32}", token) or owner != run_id or type(offset) is not int or offset <= 0:
                raise ValueError()
            path = cache / (token + ".json")
            serialized = path.read_text()
            if hashlib.sha256(serialized.encode()).hexdigest() != digest or offset >= len(serialized) or json.loads(serialized).get("workflow_run_id") != run_id:
                raise ValueError()
        except (ValueError, KeyError, TypeError, UnicodeError, OSError) as exc:
            raise ValueError("Invalid or expired Monitor snapshot cursor; refresh the workflow") from exc
    else:
        if not isinstance(run, dict) or run.get("workflow_run_id") != run_id:
            raise ValueError("Monitor snapshot requires the requested workflow run")
        serialized = json.dumps(run, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        token = uuid.uuid4().hex
        digest = hashlib.sha256(serialized.encode()).hexdigest()
        path = cache / (token + ".json")
        if len(serialized) > CHUNK_SIZE:
            with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as stream:
                stream.write(serialized)
    chunk = serialized[offset:offset + CHUNK_SIZE]
    end = offset + len(chunk)
    following = base64.urlsafe_b64encode(json.dumps({"snapshot": token, "offset": end, "digest": digest, "run": run_id}).encode()).decode() if end < len(serialized) else None
    if following is None:
        path.unlink(missing_ok=True)
    return {"monitor_snapshot": True, "workflow_run_id": run_id, "content_sha256": digest, "offset": offset, "total_characters": len(serialized), "chunk": chunk, "next_cursor": following}
