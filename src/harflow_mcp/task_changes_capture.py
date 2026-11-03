"""Capture of a settled task's repository differences into `{task_id}.changes.json`.

Runs once in the supervising owner after settlement. Everything it reads about a repository comes
from the persisted `outcome.repositories[]`; the agent-writable working tree is never asked where its
Git directory is, and Git never runs with a repository's own configuration: every command uses a
disposable Git directory (`task_outcomes.build_isolated_git_dir`) with a private copy of the index.
The capture is bounded (one monotonic deadline per task, streamed output with a hard ceiling) and
never changes settlement or `outcome`.
"""
from __future__ import annotations

import base64
import fcntl
import json
import logging
import os
import selectors
import signal
import stat
import struct
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from . import store
from . import task_changes_document as document_shape
from .state_access import open_descriptor, task_layout
from .state_ownership import StateOwnershipError
from .task_outcomes import build_isolated_git_dir
from .wire_contracts import PRODUCT, PRODUCT_VERSION

log = logging.getLogger(__name__)

BUDGET_SECONDS = 2.0
FILE_TEXT_LIMIT = document_shape.MAX_TEXT_BYTES
TASK_TEXT_LIMIT = 1024 * 1024
COMMAND_CEILING = 8 * 1024 * 1024
INDEX_LIMIT = 32 * 1024 * 1024
GIT_FILE_LIMIT = 4096
LOCK_RETRY_SECONDS = 0.2
GIT: tuple[str, ...] = ("git",)

_COMMIT = frozenset("0123456789abcdef")
_STARTERS = (b"diff --git ", b"diff --cc ", b"diff --combined ", b"* Unmerged path ")
_HEAD_BYTES = 24
_HEADER_LIMIT = 2 * document_shape.MAX_PATH_BYTES + 64
_STATUSES = {"A": "added", "M": "modified", "D": "deleted", "T": "typechange", "U": "unmerged"}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp() -> str:
    return _now().isoformat()


class _Budget:
    """One monotonic deadline and one text allowance shared by every repository of a task."""

    def __init__(self, seconds: float) -> None:
        self.deadline = time.monotonic() + seconds
        self.text_left = TASK_TEXT_LIMIT

    def left(self) -> float:
        return self.deadline - time.monotonic()

    def expired(self) -> bool:
        return self.left() <= 0


@dataclass
class _Result:
    returncode: int | None
    cause: str | None = None  # capture_timeout | output_ceiling | spawn_failed


def _kill(process: subprocess.Popen) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    try:
        process.kill()
    except OSError:
        pass


def _run(argv: list[str], environment: dict[str, str], cwd: str, budget: _Budget, sink: Callable[[bytes], None]) -> _Result:
    """Run one command, streaming stdout into `sink`; killed on the deadline or the output ceiling."""
    if budget.expired():
        return _Result(None, "capture_timeout")
    try:
        process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL,
                                   env=environment, cwd=cwd, start_new_session=True)
    except OSError:
        return _Result(None, "spawn_failed")
    cause = None
    total = 0
    selector = selectors.DefaultSelector()
    descriptor = process.stdout.fileno()
    try:
        os.set_blocking(descriptor, False)
        selector.register(descriptor, selectors.EVENT_READ)
        while True:
            remaining = budget.left()
            if remaining <= 0:
                cause = "capture_timeout"
                break
            if not selector.select(min(remaining, 0.05)):
                continue
            try:
                chunk = os.read(descriptor, 65536)
            except BlockingIOError:
                continue
            if not chunk:
                break
            if total + len(chunk) > COMMAND_CEILING:
                sink(chunk[:COMMAND_CEILING - total])
                cause = "output_ceiling"
                break
            total += len(chunk)
            sink(chunk)
        if cause is None:
            try:
                process.wait(timeout=max(budget.left(), 0.001))
            except subprocess.TimeoutExpired:
                cause = "capture_timeout"
    finally:
        selector.close()
        if cause is not None or process.poll() is None:
            _kill(process)
        process.wait()
        process.stdout.close()
    return _Result(None if cause else process.returncode, cause)


class _Context:
    """A disposable Git directory plus the hardened command line and environment for one repository."""

    def __init__(self, root: Path, work_tree: str | None, budget: _Budget) -> None:
        self.root = root
        self.git_dir = root / "git"
        self.work_tree = work_tree
        self.budget = budget
        inherited = {key: value for key, value in os.environ.items() if not key.startswith("GIT_") or key == "GIT_EXEC_PATH"}
        self.environment = {**inherited, "GIT_NO_REPLACE_OBJECTS": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
                            "GIT_CONFIG_SYSTEM": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_COUNT": "0",
                            "GIT_COMMON_DIR": str(self.git_dir), "GIT_OPTIONAL_LOCKS": "0", "GIT_NO_LAZY_FETCH": "1",
                            "GIT_TERMINAL_PROMPT": "0", "GIT_PAGER": "cat", "GIT_ATTR_NOSYSTEM": "1"}

    def command(self, *args: str, work_tree: bool = False) -> list[str]:
        return [*GIT, "--no-optional-locks", "--no-replace-objects", "--literal-pathspecs", "--git-dir=" + str(self.git_dir),
                *(("--work-tree=" + self.work_tree,) if work_tree and self.work_tree else ()),
                "-c", "core.fsmonitor=false", "-c", "core.hooksPath=/dev/null", "-c", "submodule.recurse=false",
                "-c", "diff.external=", "-c", "core.quotepath=false", "-c", "diff.noprefix=false",
                "-c", "diff.renames=false", "-c", "core.untrackedCache=false", *args]

    def run(self, args: list[str], sink: Callable[[bytes], None]) -> _Result:
        return _run(args, self.environment, str(self.root), self.budget, sink)

    def collect(self, args: list[str]) -> tuple[bytes, _Result]:
        buffer = bytearray()
        result = self.run(args, buffer.extend)
        return bytes(buffer), result


# --- inputs -----------------------------------------------------------------------------------


def _valid_commit(value: Any) -> bool:
    return isinstance(value, str) and len(value) in (40, 64) and set(value) <= _COMMIT


def _text(value: Any) -> str | None:
    return value[:document_shape.MAX_FIELD_CHARS] if isinstance(value, str) else None


def _read_regular(path: str, limit: int) -> bytes | None:
    """The bytes of a regular file opened without following a final symlink, or None when absent or unsafe."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        return None
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            return None
        chunks: list[bytes] = []
        total = 0
        while total <= limit:
            chunk = os.read(descriptor, min(1_048_576, limit + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        return b"".join(chunks) if total <= limit else None
    except OSError:
        return None
    finally:
        os.close(descriptor)


def _is_directory(path: str) -> bool:
    try:
        return stat.S_ISDIR(os.lstat(path).st_mode)
    except OSError:
        return False


def verified_git_dir(worktree: str, common: str) -> str | None:
    """The per-worktree Git directory, proven from files rather than asked of the working tree.

    A linked worktree's `.git` file must name a directory under `<common>/worktrees/` whose `commondir`
    leads back to `common` and whose `gitdir` points at that same `.git` file. A main checkout's `.git`
    must be a real directory lexically equal to `common`. Anything else is None.
    """
    dotgit = os.path.join(worktree, ".git")
    try:
        mode = os.lstat(dotgit).st_mode
    except OSError:
        return None
    if stat.S_ISDIR(mode):
        return common if os.path.normpath(dotgit) == os.path.normpath(common) else None
    if not stat.S_ISREG(mode):
        return None
    data = _read_regular(dotgit, GIT_FILE_LIMIT)
    if data is None:
        return None
    try:
        first = data.decode("utf-8").splitlines()[0]
    except (UnicodeDecodeError, IndexError):
        return None
    if not first.startswith("gitdir:"):
        return None
    target = os.path.normpath(os.path.join(worktree, first[len("gitdir:"):].strip()))
    if os.path.dirname(target) != os.path.join(os.path.normpath(common), "worktrees") or not _is_directory(target):
        return None
    commondir = _read_regular(os.path.join(target, "commondir"), GIT_FILE_LIMIT)
    back = _read_regular(os.path.join(target, "gitdir"), GIT_FILE_LIMIT)
    if commondir is None or back is None:
        return None
    try:
        resolved = os.path.join(target, commondir.decode("utf-8").strip())
        pointer = os.path.join(target, back.decode("utf-8").strip())
    except UnicodeDecodeError:
        return None
    if os.path.realpath(resolved) != os.path.realpath(common) or os.path.realpath(pointer) != os.path.realpath(dotgit):
        return None
    return target


# --- split index ------------------------------------------------------------------------------


class _IndexUnsupported(Exception):
    pass


def _shared_index_name(data: bytes, hash_length: int, budget: _Budget) -> str | None:
    """The `sharedindex.<oid>` name a split index (`link` extension) refers to, or None for a plain index."""
    if len(data) < 12 + hash_length or data[:4] != b"DIRC":
        raise _IndexUnsupported("signature")
    version, count = struct.unpack(">II", data[4:12])
    if version not in (2, 3, 4):
        raise _IndexUnsupported("version")
    end = len(data) - hash_length
    position = None
    eoie = 8 + 4 + hash_length
    if end - eoie >= 12 and data[end - eoie:end - eoie + 4] == b"EOIE":
        offset = struct.unpack(">I", data[end - eoie + 8:end - eoie + 12])[0]
        if 12 <= offset <= end - eoie:
            position = offset
    if position is None:
        position = 12
        fixed = 40 + hash_length + 2
        for index in range(count):
            if index % 8192 == 0 and budget.expired():
                raise _IndexUnsupported("deadline")
            if position + fixed > end:
                raise _IndexUnsupported("entries")
            flags = struct.unpack(">H", data[position + fixed - 2:position + fixed])[0]
            start = position
            position += fixed + (2 if flags & 0x4000 and version >= 3 else 0)
            if version == 4:
                while position < end and data[position] & 0x80:
                    position += 1
                position += 1
                nul = data.find(b"\0", position, end)
                if nul < 0:
                    raise _IndexUnsupported("entries")
                position = nul + 1
            else:
                nul = data.find(b"\0", position, end)
                if nul < 0:
                    raise _IndexUnsupported("entries")
                position = start + ((position - start + (nul - position) + 8) & ~7)
    while position + 8 <= end:
        signature = data[position:position + 4]
        size = struct.unpack(">I", data[position + 4:position + 8])[0]
        if position + 8 + size > end:
            raise _IndexUnsupported("extension")
        if signature == b"link":
            if size < hash_length:
                raise _IndexUnsupported("link")
            return "sharedindex." + data[position + 8:position + 8 + hash_length].hex()
        position += 8 + size
    return None


# --- listing and patch parsing ----------------------------------------------------------------


@dataclass
class _Entry:
    path: bytes
    status: str
    additions: int | None = None
    deletions: int | None = None
    binary: bool = False
    forced: str | None = None  # reason a row never carries text: untracked, submodule, unmerged
    rowed: bool = True
    row: dict[str, Any] = field(default_factory=dict)


def _parse_listing(data: bytes) -> list[_Entry] | None:
    """Entries of `--raw --numstat -z` output (raw records first, then one numstat record each)."""
    tokens = data.split(b"\0")
    if tokens and tokens[-1] == b"":
        tokens.pop()
    entries: list[_Entry] = []
    modes: list[tuple[bytes, bytes]] = []
    index = 0
    while index < len(tokens) and tokens[index].startswith(b":"):
        meta = tokens[index][1:].split(b" ")
        if len(meta) != 5 or index + 1 >= len(tokens):
            return None
        letter = meta[4][:1].decode("ascii", "replace")
        entries.append(_Entry(path=tokens[index + 1], status=_STATUSES.get(letter, "unknown")))
        modes.append((meta[0], meta[1]))
        index += 2
    statistics = tokens[index:]
    if len(statistics) != len(entries):
        return None
    for entry, record, (old, new) in zip(entries, statistics, modes):
        parts = record.split(b"\t", 2)
        if len(parts) != 3 or parts[2] != entry.path:
            return None
        if parts[0] == b"-" and parts[1] == b"-":
            entry.binary = True
        else:
            try:
                entry.additions, entry.deletions = int(parts[0]), int(parts[1])
            except ValueError:
                return None
        if b"160000" in (old, new):
            entry.status, entry.forced = "submodule", "submodule"
        elif entry.status == "unmerged":
            entry.forced = "unmerged"
    return entries


class _Segment:
    def __init__(self, cap: int) -> None:
        self.cap = cap
        self.size = 0
        self.kept = bytearray()
        self.header = bytearray()
        self.header_done = False
        self.partial = False


class _PatchSplitter:
    """Splits one combined patch stream into per-file segments, retaining only a bounded prefix of each.

    A segment starts at a line beginning `diff --git `, `diff --cc `, `diff --combined ` or
    `* Unmerged path `; content lines always start with a space, `+`, `-` or a backslash, so those
    prefixes cannot occur inside a hunk. Bytes beyond a segment's allowance are counted and dropped.
    """

    def __init__(self, allowances: list[bool], budget: _Budget) -> None:
        self.allowances = allowances
        self.budget = budget
        self.segments: list[_Segment] = []
        self.current: _Segment | None = None
        self.head = bytearray()
        self.streaming = False
        self.bad = False

    def _close(self) -> None:
        if self.current is not None:
            self.budget.text_left -= len(self.current.kept)

    def _start(self) -> None:
        self._close()
        index = len(self.segments)
        allowed = index < len(self.allowances) and self.allowances[index]
        if index >= len(self.allowances):
            self.bad = True
        self.current = _Segment(min(FILE_TEXT_LIMIT, max(self.budget.text_left, 0)) if allowed else 0)
        self.segments.append(self.current)

    def _write(self, data: bytes) -> None:
        segment = self.current
        if segment is None:
            self.bad = True
            return
        if not segment.header_done and len(segment.header) < _HEADER_LIMIT:
            newline = data.find(b"\n")
            segment.header += data[:newline] if newline >= 0 else data
            if newline >= 0:
                segment.header_done = True
        segment.size += len(data)
        room = segment.cap - len(segment.kept)
        if room > 0:
            segment.kept += data[:room]

    def feed(self, data: bytes) -> None:
        position, length = 0, len(data)
        while position < length:
            if self.streaming:
                newline = data.find(b"\n", position)
                end = length if newline < 0 else newline + 1
                self._write(data[position:end])
                self.streaming = newline < 0
                position = end
                continue
            need = _HEAD_BYTES - len(self.head)
            newline = data.find(b"\n", position, position + need)
            end = newline + 1 if newline >= 0 else min(length, position + need)
            self.head += data[position:end]
            position = end
            if newline >= 0 or len(self.head) >= _HEAD_BYTES:
                head = bytes(self.head)
                self.head = bytearray()
                if head.startswith(_STARTERS):
                    self._start()
                self._write(head)
                self.streaming = newline < 0

    def finish(self, complete: bool) -> None:
        if self.head:
            head = bytes(self.head)
            self.head = bytearray()
            if head.startswith(_STARTERS):
                self._start()
            self._write(head)
        self._close()
        if not complete and self.segments:
            self.segments[-1].partial = True


def _plain_header(path: bytes) -> bytes | None:
    """The `diff --git` header line expected for a path that needs no quoting, else None."""
    if any(byte < 0x20 or byte == 0x7F or byte in (0x22, 0x5C) for byte in path):
        return None
    return b"diff --git a/" + path + b" b/" + path


def _trim_incomplete_utf8(data: bytearray) -> bytes:
    try:
        bytes(data).decode("utf-8")
        return bytes(data)
    except UnicodeDecodeError as exc:
        if exc.reason == "unexpected end of data" and exc.start >= len(data) - 3:
            return bytes(data[:exc.start])
        return bytes(data)


def _encode_path(raw: bytes) -> tuple[str | None, str | None, bool]:
    truncated = len(raw) > document_shape.MAX_PATH_BYTES
    kept = raw[:document_shape.MAX_PATH_BYTES]
    for trim in range(0, 4):
        try:
            return kept[:len(kept) - trim].decode("utf-8"), None, truncated
        except UnicodeDecodeError:
            if not truncated:
                break
    return None, base64.b64encode(kept).decode("ascii"), truncated


def _row(entry: _Entry) -> dict[str, Any]:
    path, encoded, truncated = _encode_path(entry.path)
    return {"path": path, "path_bytes_b64": encoded, "path_truncated": truncated, "old_path": None, "status": entry.status,
            "additions": entry.additions, "deletions": entry.deletions, "binary": entry.binary,
            "content": {"state": "not_captured", "reason": None, "bytes": 0}}


def _set_content(row: dict[str, Any], state: str, reason: str | None, text: bytes | None = None) -> None:
    row["content"] = {"state": state, "reason": reason, "bytes": len(text) if text else 0}
    row.pop("text_b64", None)
    if text:
        row["text_b64"] = base64.b64encode(text).decode("ascii")


def _scope(state: str, reason: str | None, rows: list[dict[str, Any]] | None = None, *, truncated: bool = False,
           working_tree: bool = False) -> dict[str, Any]:
    scope = {"state": state, "reason": reason, "row_count": len(rows or []), "rows_truncated": truncated,
             "observed_at": _stamp(), "rows": rows or []}
    if working_tree:
        scope["counts_and_patch_separate"] = True
    return scope


# --- scopes -----------------------------------------------------------------------------------


def _diff_scope(context: _Context, budget: _Budget, list_args: list[str], patch_args: list[str], *, extras: list[_Entry] | None,
                working_tree: bool) -> dict[str, Any]:
    """One scope: the row listing, then the patch text allocated to the rows in path order."""
    data, result = context.collect(list_args)
    if result.cause:
        return _scope("incomplete", result.cause, working_tree=working_tree)
    if result.returncode != 0:
        return _scope("unavailable", "git_failed", working_tree=working_tree)
    entries = _parse_listing(data)
    if entries is None:
        return _scope("unavailable", "listing_unparsable", working_tree=working_tree)
    patch_entries = list(entries)
    by_path = {entry.path: entry for entry in entries}
    for extra in extras or []:
        if extra.forced == "unmerged" and extra.path in by_path:
            by_path[extra.path].status, by_path[extra.path].forced = "unmerged", "unmerged"
        elif extra.path not in by_path:
            entries.append(extra)
            by_path[extra.path] = extra
    entries.sort(key=lambda entry: entry.path)
    truncated = len(entries) > document_shape.MAX_ROWS
    for entry in entries[document_shape.MAX_ROWS:]:
        entry.rowed = False
    rows = entries[:document_shape.MAX_ROWS]
    for entry in rows:
        entry.row = _row(entry)
    scope_state, scope_reason = "available", None

    def wants_text(entry: _Entry) -> bool:
        """False (after recording why) for a row that never carries patch text."""
        row = entry.row
        if row["path_truncated"]:
            _set_content(row, "not_captured", "path_too_long")
        elif entry.forced:
            _set_content(row, "not_captured", entry.forced)
        elif entry.binary:
            _set_content(row, "binary", None)
        else:
            return True
        return False

    wanting = {id(entry): wants_text(entry) for entry in rows}
    allowances = [entry.rowed and wanting.get(id(entry), False) for entry in patch_entries]
    if any(allowances):
        if budget.text_left <= 0:
            for entry in rows:
                if wanting[id(entry)]:
                    _set_content(entry.row, "omitted_size", "task_limit")
        elif budget.expired():
            scope_state, scope_reason = "incomplete", "capture_timeout"
            for entry in rows:
                if wanting[id(entry)]:
                    _set_content(entry.row, "not_captured", "capture_timeout")
        else:
            scope_state, scope_reason = _apply_patch(context, budget, patch_args, patch_entries, allowances)
    return _scope(scope_state, scope_reason, [entry.row for entry in rows], truncated=truncated, working_tree=working_tree)


def _apply_patch(context: _Context, budget: _Budget, patch_args: list[str], patch_entries: list[_Entry],
                 allowances: list[bool]) -> tuple[str, str | None]:
    splitter = _PatchSplitter(allowances, budget)
    result = context.run(patch_args, splitter.feed)
    complete = result.cause is None and result.returncode == 0
    splitter.finish(complete)
    segments = splitter.segments
    wanted = [entry for entry, allowed in zip(patch_entries, allowances) if allowed]
    failure = result.cause or ("patch_failed" if result.returncode != 0 else None)

    def discard(reason: str) -> None:
        for segment in segments:
            budget.text_left += len(segment.kept)
        for entry in wanted:
            _set_content(entry.row, "not_captured", reason)

    if failure is None and (splitter.bad or len(segments) != len(patch_entries)):
        discard("patch_alignment_failed")
        return "incomplete", "patch_alignment_failed"
    if failure == "patch_failed":
        discard("patch_failed")
        return "incomplete", "patch_failed"
    for entry, segment in zip(patch_entries, segments):
        expected = _plain_header(entry.path)
        header = bytes(segment.header)
        if expected is not None and len(expected) <= _HEADER_LIMIT and header.startswith(b"diff --git ") and header != expected:
            discard("patch_alignment_failed")
            return "incomplete", "patch_alignment_failed"
    for index, entry in enumerate(patch_entries):
        if not allowances[index]:
            continue
        if index >= len(segments):
            _set_content(entry.row, "not_captured", failure or "patch_alignment_failed")
            continue
        segment = segments[index]
        kept = _trim_incomplete_utf8(segment.kept) if segment.size > len(segment.kept) else bytes(segment.kept)
        if segment.partial:
            _set_content(entry.row, "truncated" if kept else "not_captured", failure, kept or None)
        elif segment.cap == 0:
            _set_content(entry.row, "omitted_size", "task_limit")
        elif segment.size > len(segment.kept):
            _set_content(entry.row, "truncated", "file_limit" if len(segment.kept) >= FILE_TEXT_LIMIT else "task_limit", kept)
        else:
            _set_content(entry.row, "available", None, kept)
    return ("incomplete", failure) if failure else ("available", None)


def _committed_scope(context: _Context, budget: _Budget, base: Any, head: Any) -> dict[str, Any]:
    if not _valid_commit(head):
        return _scope("not_captured", "head_missing")
    if not _valid_commit(base) or len(base) != len(head):
        return _scope("not_captured", "base_missing")
    for commit, reason in ((head, "head_missing"), (base, "base_missing")):
        _, found = context.collect(context.command("cat-file", "-e", commit + "^{commit}"))
        if found.cause:
            return _scope("incomplete", found.cause)
        if found.returncode != 0:
            return _scope("not_captured", reason)
    _, ancestry = context.collect(context.command("merge-base", "--is-ancestor", base, head))
    if ancestry.cause:
        return _scope("incomplete", ancestry.cause)
    if ancestry.returncode == 1:
        return _scope("not_captured", "base_not_ancestor")
    if ancestry.returncode != 0:
        return _scope("unavailable", "git_failed")
    options = ["--no-ext-diff", "--no-textconv", "--no-color", "--no-renames"]
    listing = context.command("diff-tree", "-r", "-z", "--raw", "--numstat", *options, base, head, "--")
    patch = context.command("diff-tree", "-r", "-p", *options, base, head, "--")
    return _diff_scope(context, budget, listing, patch, extras=None, working_tree=False)


def _copy_index(context: _Context, git_dir: str, common: str, hash_length: int, budget: _Budget) -> str | None:
    """Copy the index (and a split index's shared part) into the disposable directory; a reason on failure."""
    index = Path(git_dir) / "index"
    data = None
    try:
        info = os.lstat(index)
    except FileNotFoundError:
        info = None
    except OSError:
        return "index_unreadable"
    if info is not None:
        if not stat.S_ISREG(info.st_mode):
            return "index_unreadable"
        if info.st_size > INDEX_LIMIT:
            return "index_too_large"
        data = _read_regular(str(index), INDEX_LIMIT)
        if data is None:
            return "index_unreadable"
    if data is not None:
        try:
            shared = _shared_index_name(data, hash_length, budget)
        except _IndexUnsupported:
            return "index_unsupported"
        if shared is not None:
            for directory in (git_dir, common):
                path = os.path.join(directory, shared)
                if not os.path.lexists(path):
                    continue
                if os.path.getsize(path) > INDEX_LIMIT:
                    return "index_too_large"
                text = _read_regular(path, INDEX_LIMIT)
                if text is None:
                    return "index_unsupported"
                _private_write(context.git_dir / shared, text)
                break
            else:
                return "index_unsupported"
        _private_write(context.git_dir / "index", data)
    context.environment["GIT_INDEX_FILE"] = str(context.git_dir / "index")
    return None


def _private_write(path: Path, data: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        view = memoryview(data)
        while view:
            view = view[os.write(descriptor, view):]
    finally:
        os.close(descriptor)


def _working_tree_scope(context: _Context, budget: _Budget, head: Any, git_dir: str | None, common: str) -> dict[str, Any]:
    if not _valid_commit(head):
        return _scope("not_captured", "head_missing", working_tree=True)
    if git_dir is None:
        return _scope("not_captured", "git_dir_unverified", working_tree=True)
    reason = _copy_index(context, git_dir, common, len(head) // 2, budget)
    if reason is not None:
        return _scope("not_captured", reason, working_tree=True)
    refresh = context.command("update-index", "-q", "--unmerged", "--ignore-submodules", "--refresh", work_tree=True)
    _, refreshed = context.collect(refresh)
    if refreshed.cause or refreshed.returncode != 0:
        # Without a refreshed copy, stat-dirty entries would be reported as modified; report none.
        return _scope("incomplete", "index_refresh_failed", working_tree=True)
    unmerged_data, unmerged_result = context.collect(context.command("ls-files", "-z", "--unmerged", work_tree=True))
    others_data, others_result = context.collect(context.command("ls-files", "-z", "--others", "--exclude-standard", work_tree=True))
    for result in (unmerged_result, others_result):
        if result.cause:
            return _scope("incomplete", result.cause, working_tree=True)
        if result.returncode != 0:
            return _scope("unavailable", "git_failed", working_tree=True)
    extras = []
    for record in unmerged_data.split(b"\0"):
        if record:
            _, _, path = record.partition(b"\t")
            extras.append(_Entry(path=path, status="unmerged", forced="unmerged"))
    for path in others_data.split(b"\0"):
        if path:
            extras.append(_Entry(path=path, status="untracked", forced="untracked"))
    options = ["--no-ext-diff", "--no-textconv", "--no-color", "--no-renames", "--ignore-submodules=dirty"]
    listing = context.command("diff-index", "-z", "--raw", "--numstat", *options, head, "--", work_tree=True)
    patch = context.command("diff-index", "-p", *options, head, "--", work_tree=True)
    return _diff_scope(context, budget, listing, patch, extras=extras, working_tree=True)


# --- repositories -----------------------------------------------------------------------------


def _baseline_dirty(entry: dict[str, Any]) -> Any:
    baseline = entry.get("baseline")
    status = baseline.get("working_tree_status") if isinstance(baseline, dict) else None
    return "unknown" if not isinstance(status, str) else status != ""


def _repository_state(scopes: dict[str, dict[str, Any]]) -> tuple[str, str | None]:
    states = [scope["state"] for scope in scopes.values()]
    problem = next((scope for scope in scopes.values() if scope["state"] in ("incomplete", "unavailable")), None)
    if all(state == "unavailable" for state in states):
        return "unavailable", problem["reason"] if problem else None
    if problem is not None:
        return "incomplete", problem["reason"]
    if all(state == "not_captured" for state in states):
        return "not_captured", next(iter(scopes.values()))["reason"]
    return "available", None


def _provenance(entry: dict[str, Any]) -> dict[str, Any]:
    return {"repository_root": _text(entry.get("repository_root")), "worktree": _text(entry.get("worktree") or entry.get("repository_root")),
            "git_common_dir": _text(entry.get("git_common_dir")), "base_commit": _text(entry.get("base_commit")),
            "base_provenance": _text(entry.get("base_provenance")), "head_commit": _text(entry.get("head_commit")),
            "observed_at": _stamp(), "baseline_dirty": _baseline_dirty(entry), "attribution": document_shape.ATTRIBUTION}


def _skipped(entry: dict[str, Any], state: str, reason: str) -> dict[str, Any]:
    scopes = {"committed_range": _scope("not_captured", reason), "working_tree": _scope("not_captured", reason, working_tree=True)}
    return {**_provenance(entry), "state": state, "reason": reason, "scopes": scopes}


def _capture_repository(entry: dict[str, Any], budget: _Budget) -> dict[str, Any]:
    if budget.expired():
        return _skipped(entry, "incomplete", "budget_exhausted")
    root, worktree, common = entry.get("repository_root"), entry.get("worktree") or entry.get("repository_root"), entry.get("git_common_dir")
    head = entry.get("head_commit")
    if not all(isinstance(value, str) and os.path.isabs(value) for value in (root, worktree, common)) or \
            not os.path.isdir(os.path.join(common, "objects")):
        return _skipped(entry, "unavailable", "repository_unavailable")
    git_dir = verified_git_dir(worktree, common)
    with tempfile.TemporaryDirectory(prefix="harflow_mcp-changes-") as directory:
        context = _Context(Path(directory), worktree, budget)
        context.git_dir.mkdir()
        format_name = "sha256" if _valid_commit(head) and len(head) == 64 else "sha1"
        build_isolated_git_dir(context.git_dir, git_dir or common, common, format_name)
        if not (context.git_dir / "HEAD").exists():
            (context.git_dir / "HEAD").write_text("ref: refs/heads/main\n")
        scopes = {"committed_range": _committed_scope(context, budget, entry.get("base_commit"), head),
                  "working_tree": _working_tree_scope(context, budget, head, git_dir, common)}
    state, reason = _repository_state(scopes)
    return {**_provenance(entry), "state": state, "reason": reason, "scopes": scopes}


def build_document(task_id: str, repositories: list[Any], *, budget_seconds: float | None = None) -> dict[str, Any]:
    """The `harflow.task-changes` document for a task's persisted outcome repositories."""
    budget = _Budget(BUDGET_SECONDS if budget_seconds is None else budget_seconds)
    entries = [entry for entry in repositories if isinstance(entry, dict)][:document_shape.MAX_REPOSITORIES]
    entries.sort(key=lambda entry: str(entry.get("repository_root")))
    captured = []
    for entry in entries:
        try:
            captured.append(_capture_repository(entry, budget))
        except StateOwnershipError:
            raise
        except Exception:
            log.warning("changes capture failed for one repository of task %s", task_id, exc_info=True)
            captured.append(_skipped(entry, "unavailable", "capture_error"))
    problem = next((entry for entry in captured if entry["state"] in ("incomplete", "unavailable")), None)
    capture = {"state": "incomplete", "reason": problem["reason"]} if problem else {"state": "available", "reason": None}
    return _envelope(task_id, capture, captured)


def _envelope(task_id: str, capture: dict[str, Any], repositories: list[dict[str, Any]]) -> dict[str, Any]:
    return {"product": PRODUCT, "product_version": PRODUCT_VERSION, "format": document_shape.FORMAT, "v": document_shape.VERSION,
            "task_id": task_id, "captured_at": _stamp(), "capture": capture, "repositories": repositories}


def failure_document(task_id: str, reason: str) -> dict[str, Any]:
    return _envelope(task_id, {"state": "unavailable", "reason": reason}, [])


def serialize(document: dict[str, Any]) -> bytes:
    return json.dumps(document, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


# --- publication ------------------------------------------------------------------------------


def _identity(record: store.TaskRecord) -> tuple[Any, ...]:
    return record.task_id, record.started_at, record.finished_at, record.exit_code, record.status


def _write_private(path: Path, data: bytes, layout: Any) -> None:
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        descriptor = open_descriptor(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, layout=layout)
        try:
            os.fchmod(descriptor, 0o600)
            view = memoryview(data)
            while view:
                view = view[os.write(descriptor, view):]
            os.fsync(descriptor)
        finally:
            descriptor.close()
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def publish(log_dir: Path, task_id: str, identity: tuple[Any, ...], data: bytes) -> bool:
    """Publish `data` as the task's changes document under `{task_id}.lock`; False (logged) when it was not.

    Not publishing is correct when the lock cannot be taken in a short time (record writers hold it
    too) or the record is gone or no longer the one that was captured: retention may be deleting it.
    """
    layout = task_layout(log_dir)
    path = log_dir / (store.validate_task_id(task_id) + document_shape.SUFFIX)
    lock = open_descriptor(log_dir / f"{task_id}.lock", os.O_CREAT | os.O_RDWR, 0o644, layout=layout)
    try:
        deadline = time.monotonic() + LOCK_RETRY_SECONDS
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    log.info("changes document for task %s not published: record lock busy", task_id)
                    return False
                time.sleep(0.01)
        current = store.read(log_dir, task_id, include_prompt=False)
        if current is None or _identity(current) != identity:
            log.info("changes document for task %s not published: record gone or replaced", task_id)
            return False
        _write_private(path, data, layout)
        return True
    finally:
        lock.close(unlock=True)


def capture_task_changes(log_dir: Path, task_id: str) -> str:
    """Capture and publish the task's changes document; returns what happened.

    `skipped` (nothing to capture), `published`, `not_published` (lock busy or record gone) or
    `failure_document` (an error was contained). Only `StateOwnershipError` propagates.
    """
    record = store.read(log_dir, task_id, include_prompt=False)
    if record is None or record.exit_code is None or record.status not in store.TERMINAL_RECORD_STATUSES:
        return "skipped"
    repositories = (record.outcome or {}).get("repositories")
    if not isinstance(repositories, list) or not repositories:
        return "skipped"
    identity = _identity(record)
    reason = None
    try:
        data = serialize(build_document(task_id, repositories))
        if len(data) > document_shape.MAX_DOCUMENT_BYTES:
            reason = "capture_too_large"
    except StateOwnershipError:
        raise
    except Exception:
        log.warning("changes capture failed for task %s", task_id, exc_info=True)
        reason = "capture_error"
    if reason is not None:
        data = serialize(failure_document(task_id, reason))
    try:
        published = publish(log_dir, task_id, identity, data)
    except StateOwnershipError:
        raise
    except Exception:
        log.warning("changes document for task %s could not be published", task_id, exc_info=True)
        return "not_published"
    if not published:
        return "not_published"
    return "failure_document" if reason is not None else "published"
