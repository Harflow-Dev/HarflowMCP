"""Kernel-canonical spelling helper: capability detection and fallbacks stay pure unit.

The Darwin capability is detected from the `open` flags actually available, never from
`sys.platform` — tests elsewhere monkeypatch the platform to "darwin" on Linux, where the
Darwin fcntl must not be issued. These tests force the fallback so the non-capable
behaviour is exercised on every platform, including macOS.
"""
import os
import sys

import pytest

from polybridge import canonical_paths
from polybridge.canonical_paths import canonical, canonical_component_name, kernel_canonical_supported


def test_capability_follows_the_actual_open_flags_not_the_platform_name(monkeypatch) -> None:
    monkeypatch.setattr(sys, "platform", "darwin")
    assert kernel_canonical_supported() == (hasattr(os, "O_EVTONLY") and hasattr(os, "O_SYMLINK"))


def test_fallback_retains_strict_resolve_behaviour(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(canonical_paths, "_KERNEL_CANONICAL_SUPPORTED", False)
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)
    assert canonical(link) == real.resolve(strict=True)
    with pytest.raises(OSError):
        canonical(tmp_path / "missing")


def test_fallback_keeps_supplied_component_spellings(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(canonical_paths, "_KERNEL_CANONICAL_SUPPORTED", False)
    assert canonical_component_name(tmp_path) is None
