"""Ordered durable native transport observations and terminal replay barriers."""
from __future__ import annotations
import json
import os
import stat
from pathlib import Path

from .state_access import task_operation
from .task_supervision import read_launch, _write
from . import store
from .wire_contracts import PRODUCT, PRODUCT_VERSION, compatible_line

_CURSORS = {}

def _envelope(task_id, generation, **fields):
    return dict(product=PRODUCT, product_version=PRODUCT_VERSION, format="harflow.task-observations", v=1, task_id=task_id, generation=generation, **fields)

def _qualify(value, task_id, generation):
    if not isinstance(value, dict) or value.get("product") != PRODUCT or value.get("format") != "harflow.task-observations" or type(value.get("v")) is not int or value["v"] != 1 or not compatible_line(value.get("product_version")) or value.get("task_id") != task_id or value.get("generation") != generation:
        raise ValueError("Incompatible native observation document")

MAX_LEDGER = 64 * 1024 * 1024


def _read(path, bound):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError('Native observation source is not a regular file')
        with os.fdopen(descriptor, 'rb', closefd=False) as handle:
            raw = handle.read(bound + 1)
        if len(raw) > bound:
            raise ValueError('Native observation source exceeds its bound')
        return raw
    finally:
        os.close(descriptor)


@task_operation
def append(log_dir: Path, task_id: str, generation: str, sequence: int, event: dict):
    saved = read_launch(log_dir, task_id)
    if saved is None or saved['generation'] != generation:
        raise ValueError('Native observation launch generation unavailable')
    raw = json.dumps(_envelope(task_id, generation, sequence=sequence, event=event), ensure_ascii=False, allow_nan=False).encode() + b'\n'
    descriptor = os.open(log_dir / f'{task_id}.observations.jsonl', os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_size + len(raw) > MAX_LEDGER:
            raise ValueError('Native observation ledger unavailable or exceeds its bound')
        with os.fdopen(descriptor, 'ab', closefd=False) as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(descriptor)
    finally:
        os.close(descriptor)


@task_operation
def finish(log_dir: Path, task_id: str, generation: str, watermark: int):
    _write(log_dir / f'{task_id}.observations-final.json', _envelope(task_id, generation, watermark=watermark, bytes=(log_dir / f'{task_id}.observations.jsonl').stat().st_size if watermark else 0))


@task_operation
def read(log_dir: Path, task_id: str, after: int = 0):
    store.validate_task_id(task_id)
    saved = read_launch(log_dir, task_id)
    if saved is None:
        raise ValueError('Native observation launch reservation unavailable')
    path = log_dir / f'{task_id}.observations.jsonl'
    key = str(path), saved['generation']
    previous = _CURSORS.get(key)
    offset, sequence = 0, 0
    events = []
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        descriptor = None
    size = 0
    if descriptor is not None:
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_LEDGER:
                raise ValueError('Native observation ledger unavailable or oversized')
            size = info.st_size
            source = info.st_dev, info.st_ino
            if previous and previous[:2] == (source, after):
                offset, sequence = previous[2], after
            with os.fdopen(descriptor, 'rb', closefd=False) as handle:
                handle.seek(offset)
                while len(events) < 1000:
                    line = handle.readline(2 * 1024 * 1024 + 1)
                    if not line:
                        break
                    if len(line) > 2 * 1024 * 1024:
                        raise ValueError('Native observation entry exceeds its bound')
                    if not line.endswith(b'\n'):
                        break
                    value = json.loads(line)
                    _qualify(value, task_id, saved['generation'])
                    sequence += 1
                    if type(value.get('sequence')) is not int or value['sequence'] != sequence or not isinstance(value.get('event'), dict):
                        raise ValueError('Native observation identity or sequence mismatch')
                    offset = handle.tell()
                    if sequence > after:
                        events.append(value)
            _CURSORS[key] = (source, sequence, offset)
        finally:
            os.close(descriptor)
    try:
        final = json.loads(_read(log_dir / f'{task_id}.observations-final.json', 4096))
    except FileNotFoundError:
        return events, None
    _qualify(final, task_id, saved['generation'])
    if type(final.get('watermark')) is not int or final['watermark'] < sequence or type(final.get('bytes')) is not int or final['bytes'] != size or (offset == size and final['watermark'] != sequence):
        raise ValueError('Native observation final barrier incomplete')
    return events, final['watermark']
