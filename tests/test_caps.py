"""Nested-dispatch caps: a child task must never be weaker than the parent task that spawned it.

`nested_enforcement_violation` / `check_nested_enforcement` / `check_nested_depth` are pure,
best-effort comparisons over two `Enforcement` values (see `backends/base.py`). The full matrix
below cross-checks every valid `(backend, freedom, network)` triple against every other against an
*independent* oracle — written straight from the plan's rule table, never by calling the function
under test — so a bug shared between the oracle and the implementation is the only way this could
pass while being wrong.
"""

from __future__ import annotations

import os
from itertools import product
from pathlib import Path

import pytest

from polybridge import backends
from polybridge.backends import (
    BACKENDS,
    FREEDOMS,
    NestedDispatchRefused,
    UnsupportedCapability,
    check_network,
)
from polybridge.backends.base import Enforcement, check_nested_depth, check_nested_enforcement

NETWORKS: list[bool | None] = [None, True, False]

# Every (backend, freedom, network) triple this backend actually accepts, keyed for readable test
# ids. Built once at import time — pure, no I/O.
CONFIGS: dict[tuple[str, str, bool | None], Enforcement] = {}
for _name, _backend in BACKENDS.items():
    for _freedom in FREEDOMS:
        for _network in NETWORKS:
            try:
                check_network(_backend, _freedom, _network)
            except UnsupportedCapability:
                continue
            CONFIGS[(_name, _freedom, _network)] = _backend.enforcement(_freedom, _network)

CONFIG_KEYS = list(CONFIGS)


def _id(key: tuple[str, str, bool | None]) -> str:
    name, freedom, network = key
    return f"{name}/{freedom}/net={network}"


PAIRS = list(product(CONFIG_KEYS, CONFIG_KEYS))


# --- Independent oracle ---------------------------------------------------------------------
#
# Deliberately not calling anything in `backends.base` beyond the `Enforcement` fields
# themselves — this is a second, from-scratch statement of the plan's rule table, so it fails
# independently of a bug in `nested_enforcement_violation`.

_NETWORK_RANK = {"blocked": 2, "enabled": 1, "not_controlled": 1, "unrestricted": 0}


def oracle_violation(
    parent: Enforcement,
    child: Enforcement,
    *,
    parent_backend: str,
    child_backend: str,
    parent_repo: str,
    child_repo: str,
) -> str | None:
    if parent.os_enforced and not child.os_enforced:
        return "os_enforced"
    if parent.writes_confined and not child.writes_confined:
        return "writes_confined"
    if parent.commit_push_blocked and not child.commit_push_blocked:
        return "commit_push_blocked"
    if parent.direct_commit_commands_denied and not child.direct_commit_commands_denied:
        return "direct_commit_commands_denied"
    if not parent.publish_attempts_allowed_by_polybridge and child.publish_attempts_allowed_by_polybridge:
        return "publish_attempts_allowed_by_polybridge"

    parent_rank = _NETWORK_RANK.get(parent.network_access, 2)
    child_rank = _NETWORK_RANK.get(child.network_access, 0)
    if child_rank < parent_rank:
        return "network_access"

    if parent.writes_confined:
        if child_backend != parent_backend:
            return "backend"
        if not set(child.writable_roots) <= set(parent.writable_roots):
            return "writable_roots"
        parent_real = os.path.realpath(parent_repo)
        child_real = os.path.realpath(child_repo)
        if child_real != parent_real and os.path.commonpath([parent_real, child_real]) != parent_real:
            return "repo"

    return None


SAME_REPO = "/tmp/polybridge-caps-test-repo"


@pytest.mark.parametrize("parent_key,child_key", PAIRS, ids=[f"{_id(p)}->{_id(c)}" for p, c in PAIRS])
def test_matrix_matches_independent_oracle(parent_key, child_key) -> None:
    parent_name, _, _ = parent_key
    child_name, _, _ = child_key
    parent_enforcement = CONFIGS[parent_key]
    child_enforcement = CONFIGS[child_key]

    expected_rule = oracle_violation(
        parent_enforcement,
        child_enforcement,
        parent_backend=parent_name,
        child_backend=child_name,
        parent_repo=SAME_REPO,
        child_repo=SAME_REPO,
    )

    result = backends.nested_enforcement_violation(
        parent_enforcement.as_dict(),
        child_enforcement,
        parent_backend=parent_name,
        child_backend=child_name,
        parent_repo=SAME_REPO,
        child_repo=SAME_REPO,
    )

    if expected_rule is None:
        assert result is None
    else:
        assert result is not None
        rule, message = result
        assert rule == expected_rule
        assert expected_rule in message


# --- Named cases -------------------------------------------------------------------------------


def test_codex_read_only_parent_refuses_codex_write_in_repo_child_on_writable_roots() -> None:
    codex = BACKENDS["codex"]
    parent = codex.enforcement("read_only").as_dict()
    child = codex.enforcement("write_in_repo")

    result = backends.nested_enforcement_violation(
        parent, child, parent_backend="codex", child_backend="codex",
        parent_repo=SAME_REPO, child_repo=SAME_REPO,
    )

    assert result is not None
    rule, _ = result
    assert rule == "writable_roots"

    with pytest.raises(NestedDispatchRefused, match="writable_roots") as exc_info:
        check_nested_enforcement(
            parent, child, parent_backend="codex", child_backend="codex",
            parent_repo=SAME_REPO, child_repo=SAME_REPO,
        )
    assert exc_info.value.rule == "writable_roots"


@pytest.mark.parametrize("parent_freedom", ["read_only", "write_in_repo", "publish"])
@pytest.mark.parametrize("child_name", ["claude", "opencode", "vibe", "antigravity"])
def test_cross_backend_child_under_confined_codex_parent_refused_on_backend(
    parent_freedom: str, child_name: str
) -> None:
    """Isolates the `backend` rule specifically: `child_backend` is a caller-supplied label, not
    derived from the child's `Enforcement` fields, so giving the child *codex's own* enforcement
    values (identical to the parent's) but claiming a different backend name still has to be
    caught — every field-level rule above it passes by construction, so only `backend` can fire.
    A real non-codex backend would additionally fail `os_enforced` first (only codex declares an
    OS sandbox), which is exactly why this isolates the label rather than using a real child
    backend's own enforcement()."""
    codex = BACKENDS["codex"]
    parent_enforcement = codex.enforcement(parent_freedom)
    parent = parent_enforcement.as_dict()
    child = parent_enforcement  # same values; only the claimed backend name differs

    result = backends.nested_enforcement_violation(
        parent, child, parent_backend="codex", child_backend=child_name,
        parent_repo=SAME_REPO, child_repo=SAME_REPO,
    )

    assert result is not None
    rule, message = result
    assert rule == "backend"
    assert "backend" in message


def test_unconfined_parent_imposes_no_backend_constraint() -> None:
    """codex `unrestricted` is not confined, so a cross-backend child under it is fine — the
    backend/writable_roots/repo trio only fires when the parent itself is confined."""
    codex = BACKENDS["codex"]
    claude = BACKENDS["claude"]
    parent = codex.enforcement("unrestricted").as_dict()
    child = claude.enforcement("unrestricted")

    result = backends.nested_enforcement_violation(
        parent, child, parent_backend="codex", child_backend="claude",
        parent_repo=SAME_REPO, child_repo="/somewhere/else/entirely",
    )

    assert result is None


def test_child_repo_outside_parent_repo_refused_on_repo(tmp_path: Path) -> None:
    codex = BACKENDS["codex"]
    parent_repo = tmp_path / "parent"
    child_repo = tmp_path / "sibling"
    parent_repo.mkdir()
    child_repo.mkdir()
    parent = codex.enforcement("write_in_repo").as_dict()
    child = codex.enforcement("write_in_repo")

    result = backends.nested_enforcement_violation(
        parent, child, parent_backend="codex", child_backend="codex",
        parent_repo=str(parent_repo), child_repo=str(child_repo),
    )

    assert result is not None
    rule, _ = result
    assert rule == "repo"


def test_child_repo_via_symlink_to_outside_still_refused_on_repo(tmp_path: Path) -> None:
    codex = BACKENDS["codex"]
    parent_repo = tmp_path / "parent"
    outside = tmp_path / "outside"
    parent_repo.mkdir()
    outside.mkdir()
    symlink = parent_repo / "escape"
    symlink.symlink_to(outside)
    parent = codex.enforcement("write_in_repo").as_dict()
    child = codex.enforcement("write_in_repo")

    result = backends.nested_enforcement_violation(
        parent, child, parent_backend="codex", child_backend="codex",
        parent_repo=str(parent_repo), child_repo=str(symlink),
    )

    assert result is not None
    rule, _ = result
    assert rule == "repo"


def test_child_repo_equal_to_parent_repo_accepted(tmp_path: Path) -> None:
    codex = BACKENDS["codex"]
    repo = tmp_path / "repo"
    repo.mkdir()
    parent = codex.enforcement("write_in_repo").as_dict()
    child = codex.enforcement("write_in_repo")

    result = backends.nested_enforcement_violation(
        parent, child, parent_backend="codex", child_backend="codex",
        parent_repo=str(repo), child_repo=str(repo),
    )

    assert result is None


def test_child_repo_nested_under_parent_repo_accepted(tmp_path: Path) -> None:
    codex = BACKENDS["codex"]
    parent_repo = tmp_path / "parent"
    child_repo = parent_repo / "nested" / "child"
    child_repo.mkdir(parents=True)
    parent = codex.enforcement("write_in_repo").as_dict()
    child = codex.enforcement("write_in_repo")

    result = backends.nested_enforcement_violation(
        parent, child, parent_backend="codex", child_backend="codex",
        parent_repo=str(parent_repo), child_repo=str(child_repo),
    )

    assert result is None


def test_child_repo_via_symlink_that_resolves_inside_parent_is_still_accepted(tmp_path: Path) -> None:
    """A symlink is only a problem when it resolves *outside* the parent — one that resolves to
    somewhere still under the parent repo must not be refused just for being a symlink."""
    codex = BACKENDS["codex"]
    parent_repo = tmp_path / "parent"
    real_nested = parent_repo / "real-nested"
    parent_repo.mkdir()
    real_nested.mkdir()
    symlink = parent_repo / "nested"
    symlink.symlink_to(real_nested)
    parent = codex.enforcement("write_in_repo").as_dict()
    child = codex.enforcement("write_in_repo")

    result = backends.nested_enforcement_violation(
        parent, child, parent_backend="codex", child_backend="codex",
        parent_repo=str(parent_repo), child_repo=str(symlink),
    )

    assert result is None


def test_claude_write_in_repo_parent_refuses_codex_write_in_repo_child_on_commit_denial() -> None:
    """Claude denies the obvious `git commit`/`git push` invocations at write_in_repo; codex has
    no per-command deny list at all, so a codex child is weaker on that one field — checked before
    the backend/writable_roots/repo group, so that is the rule reported even though codex is also
    confined in a way claude never claims to be."""
    claude = BACKENDS["claude"]
    codex = BACKENDS["codex"]
    parent = claude.enforcement("write_in_repo").as_dict()
    child = codex.enforcement("write_in_repo")

    result = backends.nested_enforcement_violation(
        parent, child, parent_backend="claude", child_backend="codex",
        parent_repo=SAME_REPO, child_repo=SAME_REPO,
    )

    assert result is not None
    rule, _ = result
    assert rule == "direct_commit_commands_denied"


@pytest.mark.parametrize(
    "parent_network,child_network,expect_refused",
    [
        ("not_controlled", "unrestricted", True),
        ("enabled", "not_controlled", False),
        ("blocked", "enabled", True),
        ("blocked", "blocked", False),
        ("unrestricted", "blocked", False),
    ],
)
def test_network_rank_cases(parent_network: str, child_network: str, expect_refused: bool) -> None:
    from dataclasses import replace

    claude = BACKENDS["claude"]
    base_parent = claude.enforcement("read_only")
    base_child = claude.enforcement("read_only")
    parent = replace(base_parent, network_access=parent_network).as_dict()
    child = replace(base_child, network_access=child_network)

    result = backends.nested_enforcement_violation(
        parent, child, parent_backend="claude", child_backend="claude",
        parent_repo=SAME_REPO, child_repo=SAME_REPO,
    )

    if expect_refused:
        assert result is not None
        rule, _ = result
        assert rule == "network_access"
    else:
        assert result is None


def test_unknown_network_value_on_parent_is_treated_as_strictest() -> None:
    from dataclasses import replace

    claude = BACKENDS["claude"]
    parent = replace(claude.enforcement("read_only"), network_access="some-future-value").as_dict()
    child = replace(claude.enforcement("read_only"), network_access="unrestricted")

    result = backends.nested_enforcement_violation(
        parent, child, parent_backend="claude", child_backend="claude",
        parent_repo=SAME_REPO, child_repo=SAME_REPO,
    )

    assert result is not None
    assert result[0] == "network_access"


def test_unknown_network_value_on_child_is_treated_as_laxest() -> None:
    from dataclasses import replace

    claude = BACKENDS["claude"]
    parent = replace(claude.enforcement("read_only"), network_access="not_controlled").as_dict()
    child = replace(claude.enforcement("read_only"), network_access="some-future-value")

    result = backends.nested_enforcement_violation(
        parent, child, parent_backend="claude", child_backend="claude",
        parent_repo=SAME_REPO, child_repo=SAME_REPO,
    )

    assert result is not None
    assert result[0] == "network_access"


def test_unknown_network_value_matching_on_both_sides_does_not_self_cancel() -> None:
    """An unknown value is parent-rank-2/child-rank-0 on *each* side independently — so the same
    unrecognised string on both sides is still refused (2 > 0), not treated as equal."""
    from dataclasses import replace

    claude = BACKENDS["claude"]
    parent = replace(claude.enforcement("read_only"), network_access="mystery").as_dict()
    child = replace(claude.enforcement("read_only"), network_access="mystery")

    result = backends.nested_enforcement_violation(
        parent, child, parent_backend="claude", child_backend="claude",
        parent_repo=SAME_REPO, child_repo=SAME_REPO,
    )

    assert result is not None
    assert result[0] == "network_access"


def test_freedom_mechanism_and_caveats_differences_never_matter() -> None:
    from dataclasses import replace

    claude = BACKENDS["claude"]
    parent_enf = claude.enforcement("read_only")
    child_enf = claude.enforcement("read_only")
    parent = replace(
        parent_enf, freedom="something-else", mechanism="a different mechanism",
        caveats=("a caveat",),
    ).as_dict()
    child = replace(
        child_enf, freedom="yet-another-thing", mechanism="a completely different mechanism",
        caveats=(),
    )

    result = backends.nested_enforcement_violation(
        parent, child, parent_backend="claude", child_backend="claude",
        parent_repo=SAME_REPO, child_repo=SAME_REPO,
    )

    assert result is None


@pytest.mark.parametrize("key", CONFIG_KEYS, ids=[_id(k) for k in CONFIG_KEYS])
def test_identical_parent_and_child_always_accepted(key) -> None:
    name, _, _ = key
    enforcement = CONFIGS[key]

    result = backends.nested_enforcement_violation(
        enforcement.as_dict(), enforcement, parent_backend=name, child_backend=name,
        parent_repo=SAME_REPO, child_repo=SAME_REPO,
    )

    assert result is None


def test_parent_missing_a_policy_field_refuses_with_parent_enforcement_unrecorded() -> None:
    claude = BACKENDS["claude"]
    parent = claude.enforcement("read_only").as_dict()
    del parent["network_access"]
    child = claude.enforcement("unrestricted")

    result = backends.nested_enforcement_violation(
        parent, child, parent_backend="claude", child_backend="claude",
        parent_repo=SAME_REPO, child_repo=SAME_REPO,
    )

    assert result is not None
    rule, message = result
    assert rule == "parent_enforcement_unrecorded"
    assert "parent_enforcement_unrecorded" in message

    with pytest.raises(NestedDispatchRefused) as exc_info:
        check_nested_enforcement(
            parent, child, parent_backend="claude", child_backend="claude",
            parent_repo=SAME_REPO, child_repo=SAME_REPO,
        )
    assert exc_info.value.rule == "parent_enforcement_unrecorded"


def test_parent_enforcement_none_is_treated_as_fully_unrecorded() -> None:
    claude = BACKENDS["claude"]
    child = claude.enforcement("read_only")

    result = backends.nested_enforcement_violation(
        None, child, parent_backend="claude", child_backend="claude",
        parent_repo=SAME_REPO, child_repo=SAME_REPO,
    )

    assert result is not None
    assert result[0] == "parent_enforcement_unrecorded"


def test_nested_dispatch_refused_error_message_names_the_rule() -> None:
    codex = BACKENDS["codex"]
    parent = codex.enforcement("read_only").as_dict()
    child = codex.enforcement("write_in_repo")

    with pytest.raises(NestedDispatchRefused) as exc_info:
        check_nested_enforcement(
            parent, child, parent_backend="codex", child_backend="codex",
            parent_repo=SAME_REPO, child_repo=SAME_REPO,
        )

    assert exc_info.value.rule in str(exc_info.value)


def test_check_nested_enforcement_raises_nothing_when_accepted() -> None:
    """A child freedom stricter than the parent's — read_only under an unrestricted parent — must
    never be refused: every field the child reports is at least as restrictive."""
    claude = BACKENDS["claude"]
    parent = claude.enforcement("unrestricted").as_dict()
    child = claude.enforcement("read_only")

    check_nested_enforcement(
        parent, child, parent_backend="claude", child_backend="claude",
        parent_repo=SAME_REPO, child_repo=SAME_REPO,
    )


# --- Depth ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "parent_depth,max_depth",
    [(0, 2), (1, 2), (0, 1), (5, 10)],
)
def test_depth_within_budget_accepted(parent_depth: int, max_depth: int) -> None:
    check_nested_depth(parent_depth, max_depth)


@pytest.mark.parametrize(
    "parent_depth,max_depth",
    [(2, 2), (1, 1), (0, 0), (10, 5)],
)
def test_depth_beyond_budget_refused(parent_depth: int, max_depth: int) -> None:
    with pytest.raises(NestedDispatchRefused) as exc_info:
        check_nested_depth(parent_depth, max_depth)
    assert exc_info.value.rule == "depth"
    assert "depth" in str(exc_info.value)
