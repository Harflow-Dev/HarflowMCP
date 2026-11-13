"""Shared types and helpers for `Backend.list_models` (the `backend-models.v1` read).

A listing is advisory and user-scoped: it reports what the harness itself says is selectable on this machine, from
one measured source per backend, and never starts a model call. A failure of one source is a successful result with
state `unavailable` and an empty list, never "no models". Nothing here branches on a backend's name; each backend
supplies its own source and parsing.
"""
from __future__ import annotations

import os
import shutil
import stat
import time
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .. import bounded_process

# States and reasons are open vocabularies for consumers; these are the values this engine emits.
AVAILABLE = "available"
NOT_INSTALLED = "not_installed"
UNSUPPORTED = "unsupported"
UNAVAILABLE = "unavailable"

TIMEOUT = "timeout"
EXIT_NONZERO = "exit_nonzero"
AUTH_REQUIRED = "auth_required"
UNPARSEABLE = "unparseable"
SOURCE_MISSING = "source_missing"
OUTPUT_CEILING = "output_ceiling"
SPAWN_FAILED = "spawn_failed"

CLI_OUTPUT = "cli_output"
HARNESS_CACHE = "harness_cache"
DOCUMENTED_ALIASES = "documented_aliases"
CONFIG_ALIASES = "config_aliases"

MAX_MODELS = 400
MAX_ID_BYTES = 256
MAX_LABEL_BYTES = 256
LINE_LIMIT_BYTES = 256 * 1024
SOURCE_DEADLINE_SECONDS = 8.0
COMMAND_BUDGET_SECONDS = 10.0
STDOUT_CAP_BYTES = 1024 * 1024
STDERR_CAP_BYTES = 64 * 1024
FILE_CAP_BYTES = 8 * 1024 * 1024


class Budget:
    """One monotonic allowance for the whole command, shared by every subprocess it starts."""

    def __init__(self, seconds: float = COMMAND_BUDGET_SECONDS, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self.deadline = clock() + seconds

    def remaining(self) -> float:
        return self.deadline - self._clock()

    def expired(self) -> bool:
        return self.remaining() <= 0


@dataclass(frozen=True)
class RawModel:
    """One parsed row before validation. Any field may be malformed; `build_listing` decides."""

    id: Any
    label: Any
    default: bool | None = None
    effort_levels: Sequence[str] | None = None


@dataclass(frozen=True)
class ModelRow:
    id: str
    label: str
    default: bool | None = None
    effort_levels: tuple[str, ...] | None = None


@dataclass(frozen=True)
class ModelListing:
    state: str
    reason: str | None = None
    source: str | None = None
    source_observed_at: str | None = None
    models: tuple[ModelRow, ...] = ()
    truncated: bool = False
    dropped_count: int = 0


@dataclass
class ModelListContext:
    """Everything a listing reads from the world, so tests inject all of it.

    `environ` is a copy of the caller's environment, `cwd` an empty directory the caller owns, `budget` the shared
    command allowance. `runner` defaults to `bounded_process.run`, looked up at call time.
    """

    environ: Mapping[str, str]
    cwd: str
    budget: Budget = field(default_factory=Budget)
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
    runner: Callable[..., bounded_process.Completed] | None = None

    def which(self, name: str) -> str | None:
        return shutil.which(name, path=self.environ.get("PATH"))

    def run(self, argv: Sequence[str], *, stdout_cap: int = STDOUT_CAP_BYTES) -> bounded_process.Completed:
        """One bounded subprocess: at most 8 s and never past the shared budget."""
        deadline = min(self.budget.deadline, time.monotonic() + SOURCE_DEADLINE_SECONDS)
        runner = self.runner if self.runner is not None else bounded_process.run
        return runner(argv, env=self.environ, cwd=self.cwd, deadline=deadline,
                      stdout_cap=stdout_cap, stderr_cap=STDERR_CAP_BYTES)


def not_installed(source: str) -> ModelListing:
    return ModelListing(NOT_INSTALLED, source=source)


def unsupported() -> ModelListing:
    return ModelListing(UNSUPPORTED)


def unavailable(reason: str, source: str | None) -> ModelListing:
    return ModelListing(UNAVAILABLE, reason, source)


def command_failure(done: bounded_process.Completed, source: str) -> ModelListing | None:
    """The `unavailable` result for a run that did not exit 0, or None when it did."""
    if done.cause is not None:
        reason = {bounded_process.TIMEOUT: TIMEOUT, bounded_process.OUTPUT_CEILING: OUTPUT_CEILING,
                  bounded_process.SPAWN_FAILED: SPAWN_FAILED}.get(done.cause, SPAWN_FAILED)
        return unavailable(reason, source)
    if done.returncode != 0:
        return unavailable(EXIT_NONZERO, source)
    return None


def decode(data: bytes) -> str | None:
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return None


def intersect_levels(source_levels: Sequence[Any] | None, backend_levels: Sequence[str]) -> tuple[str, ...] | None:
    """Source levels limited to the ones the backend accepts, in the backend's order.

    None means the source says nothing about levels; an empty tuple means it lists levels and none is selectable.
    """
    if source_levels is None:
        return None
    wanted = {level for level in source_levels if isinstance(level, str)}
    return tuple(level for level in backend_levels if level in wanted)


def _plain_text(value: str) -> bool:
    return not any(ch.isspace() or unicodedata.category(ch).startswith("C") for ch in value)


def _utf8_length(value: str) -> int | None:
    """Encoded length, or None for text that cannot be UTF-8 encoded (a lone surrogate)."""
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError:
        return None


def _valid_id(value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    length = _utf8_length(value)
    return length is not None and length <= MAX_ID_BYTES and _plain_text(value)


def _valid_label(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    length = _utf8_length(value)
    return (length is not None and length <= MAX_LABEL_BYTES
            and not any(unicodedata.category(ch).startswith("C") for ch in value))


def build_listing(rows: Sequence[RawModel], *, source: str, source_observed_at: str | None = None,
                  extra_dropped: int = 0) -> ModelListing:
    """Validate rows, then cap them.

    Rows with an invalid id or label, and repeated ids, are removed and counted in `dropped_count` (an empty label
    becomes the id). The first 400 valid rows are kept; any further valid row sets `truncated` and is not counted
    as dropped.
    """
    valid: list[ModelRow] = []
    seen: set[str] = set()
    dropped = extra_dropped
    for row in rows:
        label = row.id if row.label == "" else row.label
        if not _valid_id(row.id) or not _valid_label(label) or row.id in seen:
            dropped += 1
            continue
        seen.add(row.id)
        levels = None if row.effort_levels is None else tuple(row.effort_levels)
        valid.append(ModelRow(row.id, label, row.default, levels))
    return ModelListing(AVAILABLE, None, source, source_observed_at, tuple(valid[:MAX_MODELS]),
                        len(valid) > MAX_MODELS, dropped)


def mark_defaults(rows: Sequence[RawModel]) -> list[RawModel]:
    """Rows marked True stay True and the rest become False; with no row marked True every default is None."""
    if not any(row.default is True for row in rows):
        return [RawModel(row.id, row.label, None, row.effort_levels) for row in rows]
    return [RawModel(row.id, row.label, row.default is True, row.effort_levels) for row in rows]


def format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_timestamp(value: Any) -> str | None:
    """An RFC 3339 timestamp (with an offset) normalised to UTC `Z`, else None."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return format_timestamp(parsed)


class FileMissing(Exception):
    pass


class FileUnusable(Exception):
    """Present but not a bounded regular file, or unreadable."""


def read_file(path: str, *, limit: int = FILE_CAP_BYTES, follow_final_symlink: bool = False) -> bytes:
    """Read a regular file of at most `limit` bytes without blocking on a FIFO.

    The final path component may be a symlink only when `follow_final_symlink`; either way its target must be a
    regular file. Size is checked before and after the read.
    """
    flags = os.O_RDONLY | os.O_NONBLOCK | (0 if follow_final_symlink else os.O_NOFOLLOW)
    try:
        fd = os.open(path, flags)
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise FileMissing(str(exc)) from exc
    except OSError as exc:
        raise FileUnusable(str(exc)) from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise FileUnusable("not a regular file")
        if info.st_size > limit:
            raise FileUnusable("file exceeds the read limit")
        try:
            content = os.read(fd, limit + 1)
        except OSError as exc:
            raise FileUnusable(str(exc)) from exc
        if len(content) > limit or os.fstat(fd).st_size > limit:
            raise FileUnusable("file exceeds the read limit")
        return content
    finally:
        os.close(fd)


def absolute_directory(environ: Mapping[str, str], name: str, default_under_home: str) -> str | None:
    """`environ[name]` when set (it must be absolute), else `$HOME/<default>`; None when unusable."""
    if name in environ:
        value = environ[name]
        return value if value and os.path.isabs(value) else None
    home = environ.get("HOME", "")
    if not home or not os.path.isabs(home):
        return None
    return os.path.join(home, default_under_home)


def result_document(backend: str, listing: ModelListing, observed_at: datetime) -> dict[str, Any]:
    """The `result` object of `harflow.ctl` `backend-models` for one listing."""
    return {
        "backend": backend,
        "state": listing.state,
        "reason": listing.reason,
        "source": listing.source,
        "scope": "user",
        "advisory": True,
        "harness_version": None,
        "source_observed_at": listing.source_observed_at,
        "observed_at": format_timestamp(observed_at),
        "truncated": listing.truncated,
        "dropped_count": listing.dropped_count,
        "models": [
            {"id": row.id, "label": row.label, "default": row.default,
             "effort_levels": None if row.effort_levels is None else list(row.effort_levels)}
            for row in listing.models
        ],
    }


def fit_to_line(result: dict[str, Any], encode: Callable[[dict[str, Any]], str],
                limit: int = LINE_LIMIT_BYTES) -> dict[str, Any]:
    """Remove rows from the tail until the whole printed line (without its newline) is at most `limit` bytes.

    `encode` renders a result as the exact line ctl prints. Removing a valid row sets `truncated`. Rows already
    removed by the 400 cap are part of `result["truncated"]`.
    """
    models = result["models"]
    already = bool(result["truncated"])

    def with_count(count: int) -> dict[str, Any]:
        return {**result, "models": models[:count], "truncated": already or count < len(models)}

    def size(count: int) -> int:
        return len(encode(with_count(count)).encode("utf-8"))

    if size(len(models)) <= limit:
        return with_count(len(models))
    low, high = 0, len(models)  # size(high) is over the limit; find the largest count that fits (or 0)
    while high - low > 1:
        middle = (low + high) // 2
        if size(middle) <= limit:
            low = middle
        else:
            high = middle
    return with_count(low)
