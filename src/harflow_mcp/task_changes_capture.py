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
import hashlib
import json
import logging
import os
import selectors
import shutil
import signal
import stat
import struct
import subprocess
import re
import tempfile
import threading
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
INDEX_READ_LIMIT = 32 * 1024 * 1024  # the agent's index is parsed as data only, never handed to Git
MAX_UNMERGED_NAMES = 10_000
GIT_FILE_LIMIT = 4096
LOCK_RETRY_SECONDS = 0.2
EXIT_GRACE_SECONDS = 0.05
REAP_SECONDS = 1.0
CONFIG_LIMIT = 1024 * 1024
MAX_STORE_ENTRIES = 100_000
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


def _reap(process: subprocess.Popen) -> None:
    try:
        process.wait()
    except Exception:
        pass


def _run(argv: list[str], environment: dict[str, str], cwd: str, budget: _Budget, sink: Callable[[bytes], None],
         merge_stderr: bool = False) -> _Result:
    """Run one command, streaming stdout into `sink`; killed on the deadline or the output ceiling."""
    if budget.expired():
        return _Result(None, "capture_timeout")
    try:
        process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT if merge_stderr else subprocess.DEVNULL, stdin=subprocess.DEVNULL,
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
            # A command that closed its output at the deadline edge has finished: give its exit a fixed
            # moment instead of whatever budget is left.
            try:
                process.wait(timeout=max(budget.left(), EXIT_GRACE_SECONDS))
            except subprocess.TimeoutExpired:
                cause = "capture_timeout"
    finally:
        selector.close()
        if cause is not None or process.poll() is None:
            _kill(process)
        try:
            process.wait(timeout=REAP_SECONDS)
        except subprocess.TimeoutExpired:
            # A process that survives SIGKILL is abandoned to a daemon reaper rather than waited on here.
            cause = cause or "capture_timeout"
            threading.Thread(target=_reap, args=(process,), daemon=True).start()
        try:
            process.stdout.close()
        except OSError:
            pass
    return _Result(None if cause else process.returncode, cause)


class _Context:
    """A disposable Git directory plus the hardened command line and environment for one repository."""

    def __init__(self, root: Path, work_tree: str | None, budget: _Budget) -> None:
        self.root = root
        self.git_dir = root / "git"
        self.work_tree = work_tree
        self.budget = budget
        inherited = {key: value for key, value in os.environ.items() if not key.startswith("GIT_") or key == "GIT_EXEC_PATH"}
        self.environment = {**inherited, "HOME": str(root), "XDG_CONFIG_HOME": str(root / "xdg"),
                            "GIT_NO_REPLACE_OBJECTS": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
                            "GIT_CONFIG_SYSTEM": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_COUNT": "0",
                            "GIT_COMMON_DIR": str(self.git_dir), "GIT_OPTIONAL_LOCKS": "0", "GIT_NO_LAZY_FETCH": "1",
                            "GIT_TERMINAL_PROMPT": "0", "GIT_PAGER": "cat", "GIT_ATTR_NOSYSTEM": "1"}

    def command(self, *args: str, work_tree: bool = False) -> list[str]:
        return [*GIT, "--no-optional-locks", "--no-replace-objects", "--literal-pathspecs", "--git-dir=" + str(self.git_dir),
                *(("--work-tree=" + self.work_tree,) if work_tree and self.work_tree else ()),
                "-c", "core.fsmonitor=false", "-c", "core.hooksPath=/dev/null", "-c", "submodule.recurse=false",
                "-c", "diff.external=", "-c", "core.quotepath=false", "-c", "diff.noprefix=false",
                "-c", "diff.renames=false", "-c", "core.untrackedCache=false", "-c", "index.threads=false", *args]

    def run(self, args: list[str], sink: Callable[[bytes], None], merge_stderr: bool = False) -> _Result:
        return _run(args, self.environment, str(self.root), self.budget, sink, merge_stderr)

    def collect(self, args: list[str], merge_stderr: bool = False) -> tuple[bytes, _Result]:
        buffer = bytearray()
        result = self.run(args, buffer.extend, merge_stderr)
        return bytes(buffer), result


# --- inputs -----------------------------------------------------------------------------------


def _valid_commit(value: Any) -> bool:
    return isinstance(value, str) and len(value) in (40, 64) and set(value) <= _COMMIT


_text = document_shape.bound_field


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


# --- index ------------------------------------------------------------------------------------


class _IndexProblem(Exception):
    pass


_IGNORABLE = frozenset(list(range(0x200C, 0x2010)) + list(range(0x202A, 0x202F)) + list(range(0x206A, 0x2070)) + [0xFEFF])
_DOT_GIT = re.compile(rb"(\.git|git~1)[ .]*(:.*)?", re.DOTALL)


def _dot_git_like(component: bytes) -> bool:
    """Whether `component` is `.git` or a form Git's `verify_path` treats as it (case, NTFS dots and
    spaces, `git~1`, alternate streams, HFS+ ignorable code points)."""
    candidates = [component.lower()]
    try:
        text = component.decode("utf-8")
    except UnicodeDecodeError:
        text = None
    if text is not None:
        stripped = "".join(character for character in text if ord(character) not in _IGNORABLE)
        candidates.append(stripped.lower().encode("utf-8"))
    return any(_DOT_GIT.fullmatch(candidate) for candidate in candidates)


def unsafe_index_path(path: bytes) -> bool:
    """Whether an index entry name could address anything but a path inside the work tree.

    Mirrors Git's `verify_path`: no absolute names, empty, `.` or `..` components, no `.git` in any
    spelling Git protects against, and no backslash (`core.protectNTFS`).
    """
    if not path or path.startswith(b"/") or b"\\" in path:
        return True
    return any(component in (b"", b".", b"..") or _dot_git_like(component) for component in path.split(b"/"))


def _varint(data: bytes, position: int) -> tuple[int, int]:
    """Git's offset-encoded varint (index version 4 name prefix lengths)."""
    byte = data[position]
    position += 1
    value = byte & 0x7F
    for _ in range(4):  # a prefix length never needs more than five bytes; longer runs are hostile
        if not byte & 0x80:
            return value, position
        byte = data[position]
        position += 1
        value = ((value + 1) << 7) + (byte & 0x7F)
    if byte & 0x80:
        raise _IndexProblem("varint")
    return value, position


def _parse_unmerged(data: bytes, hash_length: int, budget: _Budget) -> set[bytes]:
    """Names of the conflicted (stage above 0) entries of an index, read as data under strict rules.

    Raises `_IndexProblem` on anything inconsistent: the checksum, an entry's declared name length
    against its NUL, non-NUL padding, names over the path bound, a split index, an unsafe conflicted
    name, or a deadline measured while walking the bytes. Nothing here is ever given to Git.
    """
    if len(data) < 12 + hash_length or data[:4] != b"DIRC":
        raise _IndexProblem("signature")
    version, count = struct.unpack(">II", data[4:12])
    end = len(data) - hash_length
    fixed = 40 + hash_length + 2
    if version not in (2, 3, 4) or _checksum(data[:end], hash_length, budget) != data[end:] or count > (end - 12) // (fixed + 1):
        raise _IndexProblem("header")
    limit = document_shape.MAX_PATH_BYTES
    position, previous, names = 12, b"", set()
    try:
        for index in range(count):
            if index % 256 == 0 and budget.expired():
                raise _IndexProblem("deadline")
            start = position
            if start + fixed > end:
                raise _IndexProblem("entries")
            flags = struct.unpack(">H", data[start + fixed - 2:start + fixed])[0]
            position += fixed
            if flags & 0x4000:
                if version < 3:
                    raise _IndexProblem("flags")
                position += 2
            if version == 4:
                strip, position = _varint(data, position)
                nul = data.find(b"\0", position, min(end, position + limit + 1))
                if strip > len(previous) or nul < 0:
                    raise _IndexProblem("name")
                name = previous[:len(previous) - strip] + data[position:nul]
                position = nul + 1
            else:
                declared = flags & 0xFFF
                nul = position + declared if declared < 0xFFF else data.find(b"\0", position, min(end, position + limit + 1))
                if nul < position or nul >= end or data[nul] != 0:
                    raise _IndexProblem("name")
                name = data[position:nul]
                stop = start + ((position - start + len(name) + 8) & ~7)
                if stop > end or b"\0" in name or any(data[nul:stop]):
                    raise _IndexProblem("padding")
                position = stop
            if not name or len(name) > limit:
                raise _IndexProblem("name")
            previous = name
            if (flags >> 12) & 3:
                names.add(name)
                if len(names) > MAX_UNMERGED_NAMES:
                    raise _IndexProblem("names")
        while position < end:
            if position + 8 > end:
                raise _IndexProblem("extension")
            signature, size = data[position:position + 4], struct.unpack(">I", data[position + 4:position + 8])[0]
            if signature == b"link" or position + 8 + size > end:
                raise _IndexProblem("extension")
            position += 8 + size
    except (struct.error, IndexError):
        raise _IndexProblem("entries") from None
    if position != end or any(unsafe_index_path(name) for name in names):
        raise _IndexProblem("trailing")
    return names


def unmerged_names(git_dir: str | None, hash_length: int, budget: _Budget) -> set[bytes] | None:
    """Conflicted names from the agent's own index, or None when they cannot be told (disclosed, not guessed)."""
    if git_dir is None:
        return None
    data = _read_regular(os.path.join(git_dir, "index"), INDEX_READ_LIMIT)
    if data is None:
        return None
    try:
        return _parse_unmerged(data, hash_length, budget)
    except _IndexProblem:
        return None


def _checksum(data: bytes, hash_length: int, budget: _Budget | None = None) -> bytes:
    """The index checksum of `data`, hashed in pieces so the deadline is honoured."""
    digest = hashlib.new("sha256" if hash_length == 32 else "sha1")
    for offset in range(0, len(data), 1 << 20):
        if budget is not None and budget.expired():
            raise _IndexProblem("deadline")
        digest.update(data[offset:offset + (1 << 20)])
    return digest.digest()



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
    raw_typechange: bool = False  # Git's own letter was `T`, whatever the row was relabelled to
    row: dict[str, Any] = field(default_factory=dict)

    @property
    def segments(self) -> int:
        """Patch segments Git prints for this entry: a type change is a deletion and an addition."""
        return 2 if self.raw_typechange or self.status == "typechange" else 1


def _parse_raw(data: bytes, *, partial: bool = False) -> list[_Entry] | None:
    """Entries of `--raw -z` output; with `partial`, only the complete records of a cut-off stream."""
    tokens = data.split(b"\0")
    last = tokens.pop()  # empty after a final NUL, otherwise an unfinished record
    entries: list[_Entry] = []
    index = 0
    while index + 1 < len(tokens) and tokens[index].startswith(b":"):
        meta = tokens[index][1:].split(b" ")
        if len(meta) != 5:
            return None
        letter = meta[4][:1].decode("ascii", "replace")
        entry = _Entry(path=tokens[index + 1], status=_STATUSES.get(letter, "unknown"), raw_typechange=letter == "T")
        if b"160000" in (meta[0], meta[1]):
            entry.status, entry.forced = "submodule", "submodule"
        elif entry.status == "unmerged":
            entry.forced = "unmerged"
        entries.append(entry)
        index += 2
    if partial:
        # A cut stream ends in an unfinished token and possibly one metadata token whose path never
        # arrived; both are dropped, every completed record is kept.
        if len(tokens) - index > 1 or (len(tokens) > index and not tokens[index].startswith(b":")):
            return None
        return entries
    if index < len(tokens) or last:
        return None
    return entries


def _parse_stat(data: bytes) -> dict[bytes, tuple[int | None, int | None, bool]] | None:
    """Counts by path from `--numstat -z` output; complete records only."""
    tokens = data.split(b"\0")
    tokens.pop()
    counts = {}
    for record in tokens:
        parts = record.split(b"\t", 2)
        if len(parts) != 3:
            return None
        if parts[0] == b"-" and parts[1] == b"-":
            counts[parts[2]] = (None, None, True)
            continue
        try:
            counts[parts[2]] = (int(parts[0]), int(parts[1]), False)
        except ValueError:
            return None
    return counts


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

    def __init__(self, allowances: list[bool], budget: _Budget, groups: list[int] | None = None) -> None:
        self.allowances = allowances
        self.groups = groups if groups is not None else list(range(len(allowances)))
        self.group_used: dict[int, int] = {}
        self.budget = budget
        self.segments: list[_Segment] = []
        self.current: _Segment | None = None
        self.head = bytearray()
        self.streaming = False
        self.bad = False

    def _close(self) -> None:
        if self.current is not None:
            self.budget.text_left -= len(self.current.kept)
            self.group_used[self.group] = self.group_used.get(self.group, 0) + len(self.current.kept)

    def _start(self) -> None:
        self._close()
        index = len(self.segments)
        allowed = index < len(self.allowances) and self.allowances[index]
        if index >= len(self.allowances):
            self.bad = True
        self.group = self.groups[index] if index < len(self.groups) else -1
        room = FILE_TEXT_LIMIT - self.group_used.get(self.group, 0)
        self.current = _Segment(max(min(room, self.budget.text_left), 0) if allowed else 0)
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
        scope["unmerged_unknown"] = True  # until the conflicts were actually examined
        scope["reported_conflicts"] = {"names": [], "truncated": False}
    return scope


# --- scopes -----------------------------------------------------------------------------------


def _diff_scope(context: _Context, budget: _Budget, raw_args: list[str], stat_args: list[str], patch_args: list[str], *,
                untracked: list[bytes] | None, working_tree: bool) -> dict[str, Any]:
    """One scope: the row listing, the counts, then the patch text allocated to the rows in path order."""
    data, listed = context.collect(raw_args)
    if not listed.cause and listed.returncode != 0:
        return _scope("unavailable", "git_failed", working_tree=working_tree)
    entries = _parse_raw(data, partial=bool(listed.cause))
    if entries is None:
        return _scope("unavailable", "listing_unparsable", working_tree=working_tree)
    problem: tuple[str, str | None] = ("incomplete", listed.cause) if listed.cause else ("available", None)
    patch_entries = list(entries)
    tracked = list(entries)
    tracked.sort(key=lambda entry: entry.path)
    others = [_Entry(path=path, status="untracked", forced="untracked") for path in sorted(untracked or [])]
    # Tracked rows are allocated first, so untracked names can never evict them.
    chosen = tracked[:document_shape.MAX_ROWS]
    room = document_shape.MAX_ROWS - len(chosen)
    dropped = tracked[document_shape.MAX_ROWS:] + others[room:]
    chosen += others[:room]
    truncated = bool(dropped) or bool(listed.cause)
    for entry in dropped:
        entry.rowed = False
    rows = sorted(chosen, key=lambda entry: entry.path)
    for entry in rows:
        entry.row = _row(entry)
    if not listed.cause:
        statistics, counted = context.collect(stat_args)
        counts = _parse_stat(statistics) if not counted.cause and counted.returncode == 0 else None
        if counts is None:
            problem = ("incomplete", counted.cause or "git_failed")
        else:
            for entry in entries:
                if entry.path in counts:
                    entry.additions, entry.deletions, entry.binary = counts[entry.path]
                    if entry.row:
                        entry.row.update(additions=entry.additions, deletions=entry.deletions, binary=entry.binary)

    def wants_text(entry: _Entry) -> bool:
        """False (after recording why) for a row that never carries patch text."""
        row = entry.row
        if row["path_truncated"]:
            _set_content(row, "not_captured", "path_too_long")
        elif entry.forced:
            _set_content(row, "not_captured", entry.forced)
        elif entry.binary:
            _set_content(row, "binary", None)
        elif listed.cause:
            _set_content(row, "not_captured", listed.cause)
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
            problem = problem if problem[0] != "available" else ("incomplete", "capture_timeout")
            for entry in rows:
                if wanting[id(entry)]:
                    _set_content(entry.row, "not_captured", "capture_timeout")
        else:
            outcome = _apply_patch(context, budget, patch_args, patch_entries, allowances)
            problem = problem if problem[0] != "available" else outcome
    return _scope(problem[0], problem[1], [entry.row for entry in rows], truncated=truncated, working_tree=working_tree)


def _apply_patch(context: _Context, budget: _Budget, patch_args: list[str], patch_entries: list[_Entry],
                 allowances: list[bool]) -> tuple[str, str | None]:
    slots: list[int] = []  # the entry index each expected patch segment belongs to
    for index, entry in enumerate(patch_entries):
        slots += [index] * entry.segments
    slot_allowed = [allowances[index] for index in slots]
    splitter = _PatchSplitter(slot_allowed, budget, slots)
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

    if failure is None and (splitter.bad or len(segments) != len(slots)):
        discard("patch_alignment_failed")
        return "incomplete", "patch_alignment_failed"
    if failure == "patch_failed":
        discard("patch_failed")
        return "incomplete", "patch_failed"
    for slot, segment in zip(slots, segments):
        expected = _plain_header(patch_entries[slot].path)
        header = bytes(segment.header)
        if expected is not None and len(expected) <= _HEADER_LIMIT and header.startswith(b"diff --git ") and header != expected:
            discard("patch_alignment_failed")
            return "incomplete", "patch_alignment_failed"
    for index, entry in enumerate(patch_entries):
        if not allowances[index]:
            continue
        own = [segment for slot, segment in zip(slots, segments) if slot == index]
        if len(own) < entry.segments:
            _set_content(entry.row, "not_captured", failure or "patch_alignment_failed")
            continue
        kept = b"".join(_trim_incomplete_utf8(segment.kept) if segment.size > len(segment.kept) else bytes(segment.kept) for segment in own)
        size = sum(segment.size for segment in own)
        if any(segment.partial for segment in own):
            _set_content(entry.row, "truncated" if kept else "not_captured", failure, kept or None)
        elif len(kept) == size:
            _set_content(entry.row, "available", None, kept)
        elif not kept:
            _set_content(entry.row, "omitted_size", "task_limit")
        else:
            _set_content(entry.row, "truncated", "file_limit" if len(kept) >= FILE_TEXT_LIMIT else "task_limit", kept)
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
    return _diff_scope(context, budget, context.command("diff-tree", "-r", "-z", "--raw", *options, base, head, "--"),
                       context.command("diff-tree", "-r", "-z", "--numstat", *options, base, head, "--"),
                       context.command("diff-tree", "-r", "-p", *options, base, head, "--"),
                       untracked=None, working_tree=False)


def _private_write(path: Path, data: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        view = memoryview(data)
        while view:
            view = view[os.write(descriptor, view):]
    finally:
        os.close(descriptor)


def _config_values(path: str, section: str, keys: frozenset[str] | None = None) -> dict[str, str]:
    """`key = value` pairs of one section of a Git config file, read as data (includes are not followed)."""
    data = _read_regular(path, CONFIG_LIMIT)
    values: dict[str, str] = {}
    if data is None:
        return values
    current = None
    for line in data.decode("utf-8", "replace").splitlines()[:20_000]:
        line = line.strip()
        if line.startswith("["):
            current = line[1:line.find("]")].strip().lower() if "]" in line else None
            continue
        if current != section or not line or line[0] in "#;":
            continue
        key, separator, value = line.partition("=")
        key = key.strip().lower()
        if keys is not None and key not in keys:
            continue
        value = re.split(r"\s[#;]", value.strip(), maxsplit=1)[0].strip().strip('"') if separator else "true"
        values[key] = value
    return values


_CORE_BOOLEANS = frozenset({"filemode", "ignorecase", "precomposeunicode", "symlinks", "autocrlf"})
_TRUE, _FALSE = frozenset({"true", "yes", "on", "1"}), frozenset({"false", "no", "off", "0"})


def _prepare_configuration(context: _Context, common: str) -> None:
    """Carry the data a faithful diff needs into the disposable directory without running any config.

    Only a fixed set of non-executing `core` booleans from the repository config, the repository's own
    `info/exclude`, and the user's global ignore file (named by `core.excludesFile` in their global
    config, else Git's default location) are copied; nothing else of any config applies.
    """
    lines = []
    for key, value in _config_values(os.path.join(common, "config"), "core", _CORE_BOOLEANS).items():
        lowered = value.lower()
        if key == "autocrlf" and lowered == "input":
            lines.append("\tautocrlf = input")
        elif lowered in _TRUE:
            lines.append(f"\t{key} = true")
        elif lowered in _FALSE:
            lines.append(f"\t{key} = false")
    exclude = _read_regular(os.path.join(common, "info", "exclude"), CONFIG_LIMIT)
    if exclude is not None:
        (context.git_dir / "info").mkdir(exist_ok=True)
        _private_write(context.git_dir / "info" / "exclude", exclude)
    home = os.environ.get("HOME") or ""
    candidates = [os.environ["GIT_CONFIG_GLOBAL"]] if os.environ.get("GIT_CONFIG_GLOBAL") else [
        os.path.join(os.environ.get("XDG_CONFIG_HOME") or os.path.join(home, ".config"), "git", "config"), os.path.join(home, ".gitconfig")]
    configured = None
    for candidate in candidates:
        configured = _config_values(candidate, "core", frozenset({"excludesfile"})).get("excludesfile", configured)
    if configured is None:
        configured = os.path.join(os.environ.get("XDG_CONFIG_HOME") or os.path.join(home, ".config"), "git", "ignore")
    if configured.startswith("~/"):
        configured = os.path.join(home, configured[2:])
    if os.path.isabs(configured):
        ignore = _read_regular(configured, CONFIG_LIMIT)
        if ignore is not None:
            _private_write(context.git_dir / "global-excludes", ignore)
            lines.append(f"\texcludesFile = {context.git_dir / 'global-excludes'}")
    if lines:
        with open(context.git_dir / "config", "a") as handle:
            handle.write("[core]\n" + "\n".join(lines) + "\n")


def _report_conflicts(names: Any) -> dict[str, Any]:
    """The advisory conflict list: at most 100 names and 8 KiB of JSON-escaped text, then `truncated`."""
    shown, used = [], 0
    ordered = sorted(names)
    for name in ordered:
        row = _row(_Entry(path=name, status="unmerged"))
        item = {"path": row["path"], "path_bytes_b64": row["path_bytes_b64"]}
        used += len(json.dumps(item)) + 2
        if len(shown) >= document_shape.MAX_REPORTED_CONFLICTS or used > document_shape.MAX_CONFLICTS_ESCAPED:
            break
        shown.append(item)
    return {"names": shown, "truncated": len(shown) < len(ordered)}


def _working_tree_scope(context: _Context, budget: _Budget, head: Any, git_dir: str | None, common: str) -> dict[str, Any]:
    """The work tree against the pinned head, through an index Git builds from the head itself.

    The agent's own index is never handed to Git: a private index is read from the head tree (Git
    rejects unsafe names while doing so), every tracked file is then hashed against it, and the diff and
    the untracked names are taken from that index. Only the conflicted names are read from the agent's
    index, as data.
    """
    if not _valid_commit(head):
        return _scope("not_captured", "head_missing", working_tree=True)
    if git_dir is None:
        # No Git command is given a work tree unless `.git` in that very directory was proven to belong to this
        # repository; a work tree named by configuration (`core.worktree`) can never pass.
        return _scope("not_captured", "git_dir_unverified", working_tree=True)
    _, found = context.collect(context.command("cat-file", "-e", head + "^{commit}"))
    if found.cause:
        return _scope("incomplete", found.cause, working_tree=True)
    if found.returncode != 0:
        return _scope("not_captured", "head_missing", working_tree=True)
    context.environment["GIT_INDEX_FILE"] = str(context.git_dir / "index")
    output, read = context.collect(context.command("read-tree", head), merge_stderr=True)
    if read.cause:
        return _scope("incomplete", read.cause, working_tree=True)
    if read.returncode != 0:
        return _scope("not_captured", "index_unsafe_path" if b"invalid path" in output else "tree_unreadable", working_tree=True)
    refresh = context.command("update-index", "-q", "--ignore-submodules", "--refresh", work_tree=True)
    _, refreshed = context.collect(refresh)
    if refreshed.cause:
        # Every tracked file is hashed here; a refresh that did not finish never yields a partial diff.
        return _scope("incomplete", refreshed.cause, working_tree=True)
    if refreshed.returncode != 0:
        return _scope("incomplete", "index_refresh_failed", working_tree=True)
    others_data, others_result = context.collect(context.command(
        "ls-files", "-z", "--others", "--exclude-standard", "--directory", "--no-empty-directory", work_tree=True))
    if not others_result.cause and others_result.returncode != 0:
        return _scope("unavailable", "git_failed", working_tree=True)
    conflicted = unmerged_names(git_dir, len(head) // 2, budget)
    listed = others_data.split(b"\0")
    listed.pop()  # the unfinished (or empty) token after the last NUL
    untracked = [path for path in listed if path]
    options = ["--no-ext-diff", "--no-textconv", "--no-color", "--no-renames", "--ignore-submodules=dirty"]
    scope = _diff_scope(context, budget, context.command("diff-index", "-z", "--raw", *options, head, "--", work_tree=True),
                        context.command("diff-index", "-z", "--numstat", *options, head, "--", work_tree=True),
                        context.command("diff-index", "-p", *options, head, "--", work_tree=True),
                        untracked=untracked, working_tree=True)
    scope["unmerged_unknown"] = conflicted is None
    # Advisory only: names from the agent's index never alter, add or suppress a row of the real diff.
    scope["reported_conflicts"] = _report_conflicts(conflicted or ())
    if others_result.cause:
        # The untracked listing was cut off: keep what was read and say so, whatever else happened.
        scope["rows_truncated"] = True
        if scope["state"] == "available":
            scope.update(state="incomplete", reason=others_result.cause)
    return scope


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
    keys = ("repository_root", "worktree", "git_common_dir", "base_commit", "base_provenance", "head_commit")
    given = {key: entry.get(key) for key in keys}
    given["worktree"] = entry.get("worktree") or entry.get("repository_root")
    bounded = {key: _text(value) for key, value in given.items()}
    return {**bounded, "observed_at": _stamp(), "baseline_dirty": _baseline_dirty(entry),
            "fields_truncated": any(isinstance(given[key], str) and bounded[key] != given[key] for key in keys),
            "attribution": document_shape.ATTRIBUTION}


def _skipped(entry: dict[str, Any], state: str, reason: str) -> dict[str, Any]:
    scopes = {"committed_range": _scope("not_captured", reason), "working_tree": _scope("not_captured", reason, working_tree=True)}
    return {**_provenance(entry), "state": state, "reason": reason, "scopes": scopes}


def _guarded(function: Callable[[], dict[str, Any]], working_tree: bool) -> dict[str, Any]:
    """One scope's capture; a failure costs that scope only, never a scope that already completed."""
    try:
        return function()
    except StateOwnershipError:
        raise
    except Exception:
        log.warning("changes capture failed for one scope", exc_info=True)
        return _scope("unavailable", "capture_error", working_tree=working_tree)


def _build_isolated(context: _Context, git_dir: str | None, common: str, object_format: str) -> None:
    """The disposable Git directory; `HEAD` is never used by the pinned commands, so an unusable one
    (not a regular file) falls back to the common directory's, then to a fixed value."""
    for source in (git_dir, common):
        if source is None:
            continue
        try:
            build_isolated_git_dir(context.git_dir, source, common, object_format)
            break
        except (ValueError, OSError):
            shutil.rmtree(context.git_dir, ignore_errors=True)
            context.git_dir.mkdir()
    else:
        build_isolated_git_dir(context.git_dir, str(context.root), common, object_format)
    if not (context.git_dir / "HEAD").exists():
        (context.git_dir / "HEAD").write_text("ref: refs/heads/main\n")


def _real(path: Any) -> str | None:
    return os.path.realpath(path) if isinstance(path, str) else None


_FANOUT = re.compile("[0-9a-f]{2}")


def _object_store_untrusted(common: str, budget: _Budget) -> tuple[str, str] | None:
    """`(state, reason)` when an object store cannot be trusted as plain content, else None.

    Applies to every repository. The store is walked lazily within the deadline and an entry bound:
    `objects` must be a real directory, every entry must be a real file or directory (never a symbolic
    link) and be one Git reads or its temporary leftovers (`info`, `pack`, lowercase two-hex fan-out
    directories, `incoming-*` quarantine directories, `tmp_obj_*` files); anything else, including a
    differently cased name, is refused. `info` is walked fully, and no alternates are accepted whatever
    the case. Hard links are not detected.
    """
    objects = os.path.join(common, "objects")
    untrusted = ("not_captured", "object_store_untrusted")
    timeout = ("incomplete", "capture_timeout")
    examined = 0

    def walk(directory: str, depth: int, kind: str) -> tuple[str, str] | None:
        nonlocal examined
        with os.scandir(directory) as entries:
            for entry in entries:
                examined += 1
                if examined > MAX_STORE_ENTRIES:
                    return untrusted
                if examined % 256 == 0 and budget.expired():
                    return timeout
                name = entry.name
                if entry.is_symlink():
                    return untrusted
                if kind == "info" and name.casefold() == "alternates":
                    return "not_captured", "alternates_present"
                if kind == "top":
                    if name in ("info", "pack") or _FANOUT.fullmatch(name):
                        if not entry.is_dir(follow_symlinks=False):
                            return untrusted
                        found = walk(entry.path, depth + 1, name if name in ("info", "pack") else "fanout")
                        if found is not None:
                            return found
                    elif name.startswith("incoming-") and entry.is_dir(follow_symlinks=False):
                        continue
                    elif name.startswith("tmp_obj_") and entry.is_file(follow_symlinks=False):
                        continue
                    else:
                        return untrusted
                elif kind == "info" and entry.is_dir(follow_symlinks=False):
                    if depth >= 3:
                        return untrusted
                    found = walk(entry.path, depth + 1, "info")
                    if found is not None:
                        return found
                elif not entry.is_file(follow_symlinks=False):
                    return untrusted
        return None

    try:
        if not stat.S_ISDIR(os.lstat(objects).st_mode) or os.path.realpath(objects) != os.path.join(os.path.realpath(common), "objects"):
            return untrusted
        if os.path.lexists(os.path.join(objects, "info", "alternates")):
            return "not_captured", "alternates_present"
        if budget.expired():
            return timeout
        return walk(objects, 0, "top")
    except OSError:
        return untrusted


def _capture_repository(entry: dict[str, Any], budget: _Budget, anchors: dict[str, str | None] | None) -> dict[str, Any]:
    if budget.expired():
        return _skipped(entry, "incomplete", "budget_exhausted")
    root, worktree, common = entry.get("repository_root"), entry.get("worktree") or entry.get("repository_root"), entry.get("git_common_dir")
    head = entry.get("head_commit")
    if not all(isinstance(value, str) and os.path.isabs(value) for value in (root, worktree, common)) or \
            not os.path.isdir(os.path.join(common, "objects")):
        return _skipped(entry, "unavailable", "repository_unavailable")
    anchors = anchors or {}
    anchored = _real(root) in anchors
    expected = anchors.get(_real(root)) if anchored else None
    if anchored and (_real(worktree) != _real(root) or (expected is not None and _real(expected) != _real(common))):
        # The repository is no longer the one dispatch recorded: nothing about it is read.
        return _skipped(entry, "not_captured", "repository_identity_changed")
    git_dir = verified_git_dir(worktree, common)
    if expected is None and anchored and git_dir is None:
        # Dispatch recorded no common directory, so only the Git directory proof can vouch for this one.
        return _skipped(entry, "not_captured", "git_dir_unverified")
    untrusted = _object_store_untrusted(common, budget)
    if untrusted is not None:
        return _skipped(entry, untrusted[0], untrusted[1])
    with tempfile.TemporaryDirectory(prefix="harflow_mcp-changes-") as directory:
        context = _Context(Path(directory), worktree, budget)
        context.git_dir.mkdir()
        _build_isolated(context, git_dir, common, "sha256" if _valid_commit(head) and len(head) == 64 else "sha1")
        _prepare_configuration(context, common)
        scopes = {"committed_range": _guarded(lambda: _committed_scope(context, budget, entry.get("base_commit"), head), False),
                  "working_tree": _guarded(lambda: _working_tree_scope(context, budget, head, git_dir, common), True)}
    state, reason = _repository_state(scopes)
    return {**_provenance(entry), "state": state, "reason": reason, "scopes": scopes}


def build_document(task_id: str, repositories: list[Any], *, budget_seconds: float | None = None,
                   anchors: dict[str, str | None] | None = None) -> dict[str, Any]:
    """The `harflow.task-changes` document for a task's persisted outcome repositories.

    `anchors` maps the real path of each repository root recorded at dispatch to the Git common
    directory it must still have (None when none was recorded).
    """
    budget = _Budget(BUDGET_SECONDS if budget_seconds is None else budget_seconds)
    entries = [entry for entry in repositories if isinstance(entry, dict)][:document_shape.MAX_REPOSITORIES]
    entries.sort(key=lambda entry: str(entry.get("repository_root")))
    captured = []
    for entry in entries:
        try:
            captured.append(_capture_repository(entry, budget, anchors))
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


def _anchors(record: store.TaskRecord) -> dict[str, str | None]:
    """The repositories the task's own dispatch records vouch for, by real path of the root, each with the
    Git common directory it must still have when that is known (a task worktree's source repository)."""
    anchors: dict[str, str | None] = {}
    baseline = record.git_baseline if isinstance(record.git_baseline, dict) else {}
    recorded = [baseline.get("repository_root")] + [item.get("repository_root") for item in baseline.get("repositories") or []
                                                      if isinstance(item, dict)]
    for root in recorded:
        if isinstance(root, str):
            anchors[os.path.realpath(root)] = None
    worktree = record.worktree if isinstance(record.worktree, dict) else {}
    if isinstance(worktree.get("path"), str) and isinstance(worktree.get("source_repo_path"), str):
        source_git = os.path.join(worktree["source_repo_path"], ".git")
        anchors[os.path.realpath(worktree["path"])] = source_git if _is_directory(source_git) else None
    return anchors


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
        data = serialize(build_document(task_id, repositories, anchors=_anchors(record)))
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
