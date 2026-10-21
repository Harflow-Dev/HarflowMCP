"""Bounded, source-checked workflow receipt acquisition and replacement guards."""
from __future__ import annotations

import json
import os
from pathlib import Path
import stat

from .catalog import source_identity
from .workflow_receipt_documents import WorkflowReceiptDocumentError, decode


def _unavailable(path: Path, reason: str) -> WorkflowReceiptDocumentError:
    return WorkflowReceiptDocumentError(path.stem, reason, code="workflow_receipt_unavailable")


def _identity(path: Path):
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise _unavailable(path, "Receipt source cannot be inspected") from exc
    if not stat.S_ISREG(info.st_mode):
        raise _unavailable(path, "Receipt source is not a regular file")
    return source_identity(info)


def check_source(path: Path, expected) -> None:
    if _identity(path) != expected:
        raise _unavailable(path, "Receipt source changed before replacement")


def read(path: Path):
    from .bounded_io import RECEIPT_BYTES, ReadLimit
    descriptor = None
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise _unavailable(path, "Receipt source is not a regular file")
        source = source_identity(opened)
        if opened.st_size > RECEIPT_BYTES:
            raise ReadLimit("JSON exceeds bounded read budget")
        with os.fdopen(os.dup(descriptor), "rb") as stream:
            content = stream.read(RECEIPT_BYTES + 1)
        if len(content) > RECEIPT_BYTES:
            raise ReadLimit("JSON exceeds bounded read budget")
        if source_identity(os.fstat(descriptor)) != source or _identity(path) != source:
            raise _unavailable(path, "Receipt source changed during read")
    except FileNotFoundError as exc:
        if descriptor is None and _identity(path) is None:
            raise
        raise _unavailable(path, "Receipt source disappeared during read") from exc
    except OSError as exc:
        raise _unavailable(path, "Receipt source cannot be read") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
    try:
        document = json.loads(content)
    except RecursionError as exc:
        raise ReadLimit("JSON nesting exceeds decoder bounds") from exc
    except (ValueError, UnicodeError) as exc:
        raise WorkflowReceiptDocumentError(path.stem, "Malformed receipt JSON") from exc
    return decode(document, path.stem), source


def observe(path: Path):
    """Qualify an existing target or capture verified initial absence."""
    try:
        return read(path)[1]
    except FileNotFoundError:
        return None
