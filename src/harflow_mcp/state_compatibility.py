"""Explicit source qualifications for private persisted formats.

This registry does not change live runtime or public wire qualification. Adding a
source line requires qualifying each listed private schema; numeric age alone
never establishes compatibility.
"""
from __future__ import annotations

from .wire_contracts import PRODUCT_VERSION, parse_version

QUALIFIED_FORMATS = frozenset({
    "harflow.store", "harflow.task", "harflow.workflow.definition",
    "harflow.workflow.run", "harflow.workflow.receipt", "harflow.task.followups",
    "harflow.task.control", "harflow.task.inbox", "harflow.task.artifacts",
    "harflow.task.approvals", "harflow.task-launch", "harflow.task-observations",
    "harflow.notifications",
})
QUALIFIED_SOURCE_LINES = {(0, 0): {name: frozenset({1}) for name in QUALIFIED_FORMATS}}


def persisted_compatible(version: str, format_name: str, schema_version: int = 1,
                         *, target_version: str | None = None) -> bool:
    """Accept current-line schema 1 or explicitly registered older source schemas."""
    source = parse_version(version)[:2]
    target = parse_version(target_version or PRODUCT_VERSION)[:2]
    if type(schema_version) is not int or source > target or format_name not in QUALIFIED_FORMATS:
        return False
    if source == target:
        return schema_version == 1
    return schema_version in QUALIFIED_SOURCE_LINES.get(source, {}).get(format_name, ())
