"""Pure public wire identities and compatibility rules for Harflow v1."""
from __future__ import annotations

import os
import platform
import re
import sys
from collections.abc import Mapping
from typing import Any

PRODUCT = "harflow"
PRODUCT_VERSION = "0.0.5"
WIRE_VERSION = 1
CTL_FORMAT = "harflow.ctl"
SETUP_FORMAT = "harflow.setup"
EVENT_FORMAT = "harflow.event"
VERSION_INFO_FORMAT = "harflow.version-info"
FORMATS = frozenset({CTL_FORMAT, SETUP_FORMAT, EVENT_FORMAT, VERSION_INFO_FORMAT})
COMMON_FIELDS = frozenset({"product", "format", "v", "product_version"})
MAX_VERSION_COMPONENT = 2_147_483_647
_VERSION = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?", re.ASCII)
CAPABILITIES = ("ctl.v1", "setup.v1", "events.v1", "version-info.v1", "state-recovery.v1", "caller-approvals.monitor-respond.v1", "statistics-page.v1", "backend-qualification.v1", "messaging.v1", "limit-hits.v1")


def parse_version(value: str) -> tuple[int, int, int]:
    """Parse a bounded stable semantic version; build metadata has no compatibility effect."""
    if not isinstance(value, str) or len(value) > 256:
        raise ValueError("Malformed product version")
    match = _VERSION.fullmatch(value)
    if match is None:
        raise ValueError("Malformed product version; prereleases are unsupported")
    components = tuple(int(part) for part in match.groups())
    if any(part > MAX_VERSION_COMPONENT for part in components):
        raise ValueError("Product version component exceeds 2147483647")
    return components


def compatible_line(actual: str, expected: str = PRODUCT_VERSION) -> bool:
    return parse_version(actual)[:2] == parse_version(expected)[:2]


def envelope(format: str, payload: Mapping[str, Any] | None = None, *, product_version: str = PRODUCT_VERSION) -> dict[str, Any]:
    """Stamp reserved common fields after payload fields so payloads cannot override identity."""
    if format not in FORMATS:
        raise ValueError("Unsupported Harflow format")
    parse_version(product_version)
    return {**(payload or {}), "product": PRODUCT, "format": format, "v": WIRE_VERSION, "product_version": product_version}


def validate_envelope(document: Mapping[str, Any], format: str, *, live: bool = True, expected_version: str = PRODUCT_VERSION) -> None:
    """Validate identity first; historical event versions are provenance, not release gates."""
    if not isinstance(document, Mapping) or document.get("product") != PRODUCT or document.get("format") != format:
        raise ValueError("Missing or foreign Harflow product/format")
    if type(document.get("v")) is not int or document["v"] != WIRE_VERSION:
        raise ValueError("Unsupported Harflow wire version")
    version = document.get("product_version")
    parse_version(version)
    if live and not compatible_line(version, expected_version):
        raise ValueError("Incompatible Harflow product major/minor line")


def version_info(environ: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Describe this engine without reading configuration, state or harnesses."""
    from .state_layout import resolve_layout
    state_root = str(resolve_layout(environ=environ).root)
    return envelope(VERSION_INFO_FORMAT, {
        "component": "engine",
        "contracts": {"ctl": {"format": CTL_FORMAT, "v": WIRE_VERSION}, "setup": {"format": SETUP_FORMAT, "v": WIRE_VERSION}, "events": {"format": EVENT_FORMAT, "v": WIRE_VERSION}},
        "capabilities": list(CAPABILITIES),
        "os": sys.platform,
        "architecture": platform.machine(),
        "state_root": state_root,
    })
