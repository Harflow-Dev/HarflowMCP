import copy

import pytest

from polybridge.workflow_migration import ParallelMigration, migrate_parallel_boundaries


def graph():
    return {"name": "Review", "revision": 12, "nodes": [
        {"id": name, "type": "end" if name == "end" else "agent", "position": {"x": i * 240, "y": 80}, "instructions": name, "agent": {"backend": "codex"}}
        for i, name in enumerate(["scope", "x", "y", "adjudicate", "end"])
    ], "connections": [
        {"id": "x", "source": "scope", "target": "x", "condition": "Review pinned input", "custom": 1},
        {"id": "y", "source": "scope", "target": "y", "condition": "Independent review"},
        {"id": "skip", "source": "scope", "target": "end", "condition": "Inputs unavailable"},
        {"id": "xa", "source": "x", "target": "adjudicate", "condition": "X finished"},
        {"id": "ya", "source": "y", "target": "adjudicate", "condition": "Y finished"},
        {"id": "finish", "source": "adjudicate", "target": "end"},
    ]}


def test_conversion_preserves_original_properties_and_alternative():
    original = graph()
    before = copy.deepcopy(original)
    result = migrate_parallel_boundaries(original, [ParallelMigration("reviews", "scope", ("x", "y"), "adjudicate", "Pinned inputs are ready")])
    assert original == before
    assert result["nodes"][:5] == original["nodes"]
    assert result["revision"] == 12
    assert result["routing_mode"] == "explicit"
    assert result["connections"][2] == original["connections"][2]
    assert result["connections"][0] == {**original["connections"][0], "source": "reviews-start"}
    assert result["connections"][3] == {**original["connections"][3], "target": "reviews-end"}
    assert all(n["position"]["y"] >= 0 for n in result["nodes"])


@pytest.mark.parametrize("change", ["escape", "overlap", "external", "unknown"])
def test_rejects_ambiguous_or_invalid_regions(change):
    d = graph()
    if change == "escape":
        d["connections"][3]["target"] = "end"
    elif change == "overlap":
        d["connections"][1]["target"] = "x"
    elif change == "external":
        d["connections"].append({"id": "external", "source": "end", "target": "x"})
    else:
        d["connections"][0]["id"] = "unknown"
    with pytest.raises(ValueError):
        migrate_parallel_boundaries(d, [ParallelMigration("reviews", "scope", ("x", "y"), "adjudicate", "Ready")])


def test_no_guessed_parallel_selection_and_no_double_migration():
    converted = migrate_parallel_boundaries(graph(), [])
    assert len(converted["nodes"]) == 5
    assert migrate_parallel_boundaries(converted, []) == converted
    with pytest.raises(ValueError, match="already"):
        migrate_parallel_boundaries(converted, [ParallelMigration("reviews", "scope", ("x", "y"), "adjudicate", "Ready")])


def test_nested_multi_step_conversion_preserves_outer_branch_identity():
    names = ["scope", "x", "inner_x", "inner_y", "inner_join", "y", "join", "end"]
    d = {"nodes": [{"id": n, "type": "end" if n == "end" else "agent", "position": {"x": 80, "y": 80}} for n in names], "connections": []}
    for edge_id, source, target in [("sx", "scope", "x"), ("sy", "scope", "y"), ("ix", "x", "inner_x"), ("iy", "x", "inner_y"), ("ixj", "inner_x", "inner_join"), ("iyj", "inner_y", "inner_join"), ("ij", "inner_join", "join"), ("yj", "y", "join"), ("finish", "join", "end")]:
        d["connections"].append({"id": edge_id, "source": source, "target": target})
    result = migrate_parallel_boundaries(d, [
        ParallelMigration("inner", "x", ("ix", "iy"), "inner_join", "Ready"),
        ParallelMigration("outer", "scope", ("sx", "sy"), "join", "Ready"),
    ])
    by_id = {e["id"]: e for e in result["connections"]}
    assert by_id["sx"]["target"] == "x"
    assert by_id["inner-exit"]["target"] == "inner_join"
    assert by_id["ij"]["target"] == "outer-end"
    assert by_id["outer-exit"]["target"] == "join"
