"""Qualified private state formats stay distinct from live wire compatibility."""
from __future__ import annotations

import pytest

from harflow_mcp import state_compatibility


@pytest.mark.parametrize("format_name", sorted(state_compatibility.QUALIFIED_FORMATS))
def test_qualified_prior_line_private_formats_are_accepted(format_name: str) -> None:
    assert state_compatibility.persisted_compatible("0.0.1", format_name)


def test_current_line_accepts_only_schema_one() -> None:
    assert state_compatibility.persisted_compatible("0.0.1", "harflow.task")
    assert not state_compatibility.persisted_compatible("0.0.1", "harflow.task", schema_version=2)


@pytest.mark.parametrize(
    ("version", "format_name", "schema_version"),
    [
        ("0.0.1", "foreign.store", 1),
        ("0.0.1", "harflow.task", 2),
        ("0.1.0", "harflow.task", 1),
        ("1.0.0", "harflow.task", 1),
        ("0.2.0", "harflow.task", 1),
    ],
)
def test_unqualified_unknown_or_newer_private_state_fails_closed(
    version: str, format_name: str, schema_version: int
) -> None:
    assert not state_compatibility.persisted_compatible(version, format_name, schema_version)


@pytest.mark.parametrize("version", ["not-a-version", "0.0", "0.0.1-alpha"])
def test_malformed_private_state_version_is_rejected(version: str) -> None:
    with pytest.raises(ValueError):
        state_compatibility.persisted_compatible(version, "harflow.task")


def test_source_line_registration_is_not_numeric_age_inference() -> None:
    # A numerically older line is still refused unless every private format was
    # explicitly qualified in the registry.
    assert not state_compatibility.persisted_compatible("0.1.0", "harflow.task")
