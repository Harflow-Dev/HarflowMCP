"""The measured model-listing sources, one function per harness (see docs/backend-cli-facts.md, "Model listing").

Each backend's `list_models` calls the function for its own source; shared types, bounds and file reading live in
`model_listing`. Every function checks installation first and reads or spawns nothing for a harness that is not on
PATH. Project and admin configuration layers are never read: the scope is the user's own.
"""
from __future__ import annotations

import json
import os
import re
import tomllib
from collections.abc import Sequence
from typing import Any

from . import model_listing as ml
from .model_listing import ModelListContext, ModelListing, RawModel


def _json_loads(data: bytes) -> Any:
    try:
        return json.loads(data)
    except (ValueError, RecursionError):
        return ml.UNPARSEABLE


def claude_aliases(context: ModelListContext, binary: str, aliases: Sequence[str]) -> ModelListing:
    """The aliases `claude --help` documents for `--model`. No subprocess: there is no listing command."""
    if context.which(binary) is None:
        return ml.not_installed(ml.DOCUMENTED_ALIASES)
    return ml.build_listing([RawModel(alias, alias.capitalize()) for alias in aliases], source=ml.DOCUMENTED_ALIASES)


def codex_cache(context: ModelListContext, binary: str, backend_levels: Sequence[str]) -> ModelListing:
    """`models_cache.json` under `$CODEX_HOME` (else `~/.codex`): rows with `visibility == "list"` by `priority`."""
    source = ml.HARNESS_CACHE
    if context.which(binary) is None:
        return ml.not_installed(source)
    directory = ml.absolute_directory(context.environ, "CODEX_HOME", ".codex")
    if directory is None:
        return ml.unavailable(ml.SOURCE_MISSING, source)
    try:
        data = ml.read_file(os.path.join(directory, "models_cache.json"))
    except ml.FileMissing:
        return ml.unavailable(ml.SOURCE_MISSING, source)
    except ml.FileUnusable:
        return ml.unavailable(ml.UNPARSEABLE, source)
    document = _json_loads(data)
    if not isinstance(document, dict) or not isinstance(document.get("models"), list):
        return ml.unavailable(ml.UNPARSEABLE, source)
    listed: list[tuple[float, RawModel]] = []
    for row in document["models"]:
        if not isinstance(row, dict) or not isinstance(row.get("visibility"), str):
            return ml.unavailable(ml.UNPARSEABLE, source)
        if row["visibility"] != "list":
            continue
        priority, levels = row.get("priority"), row.get("supported_reasoning_levels")
        if (not isinstance(row.get("slug"), str) or not isinstance(row.get("display_name"), str)
                or isinstance(priority, bool) or not isinstance(priority, (int, float))
                or not isinstance(levels, list)
                or not all(isinstance(level, dict) and isinstance(level.get("effort"), str) for level in levels)):
            return ml.unavailable(ml.UNPARSEABLE, source)
        efforts = ml.intersect_levels([level["effort"] for level in levels], backend_levels)
        listed.append((priority, RawModel(row["slug"], row["display_name"], None, efforts)))
    listed.sort(key=lambda item: item[0])  # stable: ties keep cache order
    return ml.build_listing([row for _, row in listed], source=source,
                            source_observed_at=ml.normalize_timestamp(document.get("fetched_at")))


def _opencode_objects(text: str) -> list[tuple[str, dict[str, Any]]] | None:
    """`provider/model` header lines each followed by one pretty-printed JSON object; None on any deviation."""
    decoder = json.JSONDecoder()
    position, length, found = 0, len(text), []
    while True:
        while position < length and text[position].isspace():
            position += 1
        if position >= length:
            return found
        end = text.find("\n", position)
        end = length if end < 0 else end
        header = text[position:end].strip()
        if not header or header.startswith("{") or "/" not in header or any(ch.isspace() for ch in header):
            return None
        position = end
        while position < length and text[position].isspace():
            position += 1
        try:
            value, position = decoder.raw_decode(text, position)
        except (ValueError, RecursionError):
            return None
        if not isinstance(value, dict):
            return None
        found.append((header, value))


OPENCODE_STDOUT_CAP = 32 * 1024 * 1024


def opencode_models(context: ModelListContext, binary: str, backend_levels: Sequence[str]) -> ModelListing:
    """`opencode models --verbose`: id is the header, label is `name`, levels are the `variants` keys."""
    source = ml.CLI_OUTPUT
    executable = context.which(binary)
    if executable is None:
        return ml.not_installed(source)
    done = context.run([executable, "models", "--verbose"], stdout_cap=OPENCODE_STDOUT_CAP)
    failure = ml.command_failure(done, source)
    if failure is not None:
        return failure
    text = ml.decode(done.stdout)
    objects = None if text is None else _opencode_objects(text)
    if not objects:
        return ml.unavailable(ml.UNPARSEABLE, source)
    rows: list[RawModel] = []
    for header, value in objects:
        if f"{value.get('providerID')}/{value.get('id')}" != header:
            return ml.unavailable(ml.UNPARSEABLE, source)
        variants = value.get("variants")
        levels = ml.intersect_levels(list(variants), backend_levels) if isinstance(variants, dict) and variants else None
        rows.append(RawModel(header, value.get("name"), None, levels))
    return ml.build_listing(rows, source=source)


def antigravity_models(context: ModelListContext, binary: str) -> ModelListing:
    """`agy models`: `<id>\\t<label>` per stdout line; the "Fetching" notice is on stderr."""
    source = ml.CLI_OUTPUT
    executable = context.which(binary)
    if executable is None:
        return ml.not_installed(source)
    done = context.run([executable, "models"])
    failure = ml.command_failure(done, source)
    if failure is not None:
        return failure
    text = ml.decode(done.stdout)
    if text is None:
        return ml.unavailable(ml.UNPARSEABLE, source)
    rows: list[RawModel] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        identifier, tab, label = line.partition("\t")
        if not tab:
            return ml.unavailable(ml.UNPARSEABLE, source)
        rows.append(RawModel(identifier, label.strip()))
    if not rows:
        return ml.unavailable(ml.UNPARSEABLE, source)
    return ml.build_listing(rows, source=source)


_CURSOR_ROW = re.compile(r"(\S+) - (.+)\Z")
_CURSOR_SUFFIX = re.compile(r"\s*\(((?:current|default)(?:, *(?:current|default))*)\)\Z")
_CURSOR_AUTH = re.compile(r"^Error: Authentication required\.", re.MULTILINE)


def _cursor_candidates(context: ModelListContext, names: Sequence[str], version_re: re.Pattern[str]) -> tuple[str | None, ModelListing | None]:
    """(verified executable, probe failure). Both None means no Cursor CLI is installed.

    A candidate counts as not installed only when it ran and did not print Cursor's version line. A probe that
    timed out, hit the output ceiling or failed to spawn is a failure unless a later candidate verifies.
    """
    failure: ModelListing | None = None
    for name in names:
        path = context.which(name)
        if path is None:
            continue
        done = context.run([path, "--version"])
        if done.cause is not None:
            failure = failure or ml.command_failure(done, ml.CLI_OUTPUT)
            continue
        text = ml.decode(done.stdout) or ""
        lines = text.strip().splitlines()
        if lines and version_re.fullmatch(lines[0]):
            return path, None
    return None, failure


def cursor_models(context: ModelListContext, names: Sequence[str], version_re: re.Pattern[str]) -> ModelListing:
    """`cursor-agent models`: `Available models`, then `<id> - <label>` rows, then a `Tip:` line."""
    source = ml.CLI_OUTPUT
    executable, failure = _cursor_candidates(context, names, version_re)
    if executable is None:
        return failure if failure is not None else ml.not_installed(source)
    done = context.run([executable, "models"])
    if done.cause is None and done.returncode != 0:
        combined = (ml.decode(done.stderr) or "") + (ml.decode(done.stdout) or "")
        if _CURSOR_AUTH.search(combined):
            return ml.unavailable(ml.AUTH_REQUIRED, source)
    failed = ml.command_failure(done, source)
    if failed is not None:
        return failed
    text = ml.decode(done.stdout)
    if text is None:
        return ml.unavailable(ml.UNPARSEABLE, source)
    rows: list[RawModel] = []
    header = False
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("Tip:"):
            break
        if not header:
            if line != "Available models":
                return ml.unavailable(ml.UNPARSEABLE, source)
            header = True
            continue
        match = _CURSOR_ROW.match(line)
        if match is None:
            return ml.unavailable(ml.UNPARSEABLE, source)
        label, default = match.group(2), None
        suffix = _CURSOR_SUFFIX.search(label)
        if suffix is not None:
            default = "default" in [part.strip() for part in suffix.group(1).split(",")]
            label = label[:suffix.start()]
        rows.append(RawModel(match.group(1), label, True if default else None))
    if not header or not rows:
        return ml.unavailable(ml.UNPARSEABLE, source)
    return ml.build_listing(ml.mark_defaults(rows), source=source)


def vibe_config(context: ModelListContext, binary: str) -> ModelListing:
    """`[[models]]` in `$VIBE_HOME/config.toml` (else `~/.vibe`): `alias` is the id, `name` the label."""
    source = ml.CONFIG_ALIASES
    if context.which(binary) is None:
        return ml.not_installed(source)
    directory = ml.absolute_directory(context.environ, "VIBE_HOME", ".vibe")
    if directory is None:
        return ml.unavailable(ml.SOURCE_MISSING, source)
    try:
        data = ml.read_file(os.path.join(directory, "config.toml"), follow_final_symlink=True)
        config = tomllib.loads(data.decode("utf-8"))
    except ml.FileMissing:
        return ml.unavailable(ml.SOURCE_MISSING, source)
    except (ml.FileUnusable, UnicodeDecodeError, tomllib.TOMLDecodeError, RecursionError):
        return ml.unavailable(ml.UNPARSEABLE, source)
    models = config.get("models", [])
    if not isinstance(models, list) or not all(isinstance(entry, dict) for entry in models):
        return ml.unavailable(ml.UNPARSEABLE, source)
    active = config.get("active_model")
    matched = isinstance(active, str) and any(entry.get("alias") == active for entry in models)
    rows = []
    for entry in models:
        alias, name = entry.get("alias"), entry.get("name")
        label = name if isinstance(name, str) and name else alias
        rows.append(RawModel(alias, label, (alias == active) if matched else None))
    return ml.build_listing(rows, source=source)
