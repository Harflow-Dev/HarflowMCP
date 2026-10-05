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
RECEIPT_BYTES = 4096


def _identity(stat: os.stat_result) -> list[int]:
    return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


def _remove(path: Path) -> None:
    for target in (path, path.with_suffix('.meta'), path.with_suffix('.sha')):
        target.unlink(missing_ok=True)


def _write(path: Path, content: bytes) -> None:
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as stream:
        stream.write(content)


def snapshot(run: dict | None, run_id: str, cache: Path, cursor: str | None = None, *, view: str | None = None, serialized: bytes | None = None) -> dict:
    cache.mkdir(mode=0o700, parents=True, exist_ok=True)
    now = time.time()
    for path in cache.iterdir():
        if path.suffix not in {'.json', '.meta', '.sha'}:
            continue
        try:
            if now - path.stat().st_mtime > TTL:
                _remove(path.with_suffix('.json'))
        except FileNotFoundError:
            pass
    offset = 0
    if cursor:
        try:
            if not isinstance(cursor, str) or len(cursor) > 4096:
                raise ValueError()
            decoded = json.loads(base64.urlsafe_b64decode(cursor))
            token, offset = decoded['snapshot'], decoded['offset']
            if not isinstance(token, str) or not re.fullmatch(r'[0-9a-f]{32}', token) or decoded['run'] != run_id or type(offset) is not int or offset <= 0 or offset % CHUNK_SIZE:
                raise ValueError()
            path = cache / (token + '.json')
            with path.with_suffix('.meta').open('rb') as stream:
                receipt_bytes = stream.read(RECEIPT_BYTES + 1)
            if len(receipt_bytes) > RECEIPT_BYTES or hashlib.sha256(receipt_bytes).hexdigest() != decoded['receipt']:
                raise ValueError()
            receipt = json.loads(receipt_bytes)
            digest, total = receipt['digest'], receipt['total']
            if receipt.get('view') != view or decoded.get('view') != view or receipt['run'] != run_id or digest != decoded['digest'] or type(total) is not int or not offset < total:
                raise ValueError()
            # ASCII serialization makes character offsets equal byte offsets.
            with path.open('rb') as stream, path.with_suffix('.sha').open('rb') as hashes:
                if _identity(os.fstat(stream.fileno())) != receipt['file'] or _identity(os.fstat(hashes.fileno())) != receipt['hashes']:
                    raise ValueError()
                stream.seek(offset)
                content = stream.read(min(CHUNK_SIZE, total - offset))
                hashes.seek((offset // CHUNK_SIZE) * 32)
                expected = hashes.read(32)
                if len(content) != min(CHUNK_SIZE, total - offset) or hashlib.sha256(content).digest() != expected:
                    raise ValueError()
                if _identity(os.fstat(stream.fileno())) != receipt['file'] or _identity(os.fstat(hashes.fileno())) != receipt['hashes']:
                    raise ValueError()
            chunk = content.decode('ascii')
            receipt_digest = decoded['receipt']
        except (ValueError, KeyError, TypeError, AttributeError, UnicodeError, OSError) as exc:
            raise ValueError('Invalid or expired Monitor snapshot cursor; refresh the workflow') from exc
    else:
        if not isinstance(run, dict) or run.get('workflow_run_id') != run_id:
            raise ValueError('Monitor snapshot requires the requested workflow run')
        serialized = serialized if serialized is not None else json.dumps(run, ensure_ascii=True, sort_keys=True, separators=(',', ':')).encode('ascii')
        token = uuid.uuid4().hex
        digest, total = hashlib.sha256(serialized).hexdigest(), len(serialized)
        path = cache / (token + '.json')
        receipt_digest = None
        if total > CHUNK_SIZE:
            try:
                _write(path, serialized)
                hashes_path = path.with_suffix('.sha')
                _write(hashes_path, b''.join(hashlib.sha256(serialized[start:start + CHUNK_SIZE]).digest() for start in range(0, total, CHUNK_SIZE)))
                receipt = {'run': run_id, 'view': view, 'digest': digest, 'total': total, 'file': _identity(path.stat()), 'hashes': _identity(hashes_path.stat())}
                receipt_bytes = json.dumps(receipt, separators=(',', ':')).encode('ascii')
                if len(receipt_bytes) > RECEIPT_BYTES:
                    raise ValueError('Monitor snapshot receipt exceeds its byte budget')
                _write(path.with_suffix('.meta'), receipt_bytes)
                receipt_digest = hashlib.sha256(receipt_bytes).hexdigest()
            except BaseException:
                _remove(path)
                raise
        chunk = serialized[:CHUNK_SIZE].decode('ascii')
    end = offset + len(chunk)
    following = base64.urlsafe_b64encode(json.dumps({'snapshot': token, 'offset': end, 'digest': digest, 'run': run_id, 'receipt': receipt_digest, 'view': view}, separators=(',', ':')).encode()).decode() if end < total else None
    if following is None:
        _remove(path)
    return {'monitor_snapshot': True, 'workflow_run_id': run_id, 'content_sha256': digest, 'offset': offset, 'total_characters': total, 'chunk': chunk, 'next_cursor': following}
