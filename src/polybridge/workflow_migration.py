"""Pure, explicitly approved graph conversions; storage/revision checks belong to the caller."""
from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ParallelMigration:
    """An identified parallel region, not a guess based on outgoing arrow count."""

    group_id: str
    source_id: str
    branch_edge_ids: tuple[str, ...]
    join_id: str
    condition: str
    title: str = "Parallel"


def migrate_parallel_boundaries(
    definition: dict[str, Any], groups: list[ParallelMigration]
) -> dict[str, Any]:
    """Return a converted copy preserving original IDs, properties and alternative edges.

    Original branch/arrival edge conditions stay attached to those edges. The caller
    supplies the group-entry condition explicitly because branch conditions may differ.
    Pass nested regions innermost first. Validate the returned graph before saving it
    with the source revision; this helper deliberately performs no filesystem writes.
    """
    converted = copy.deepcopy(definition)
    if converted.get("routing_mode") == "explicit":
        if groups:
            raise ValueError("Workflow already uses explicit parallel boundaries")
        return converted
    nodes = converted["nodes"]
    edges = converted["connections"]
    for group in groups:
        ids = {n["id"] for n in nodes} | {e["id"] for e in edges}
        start_id, end_id = f"{group.group_id}-start", f"{group.group_id}-end"
        entry_id, exit_id = f"{group.group_id}-enter", f"{group.group_id}-exit"
        if any(i in ids for i in (start_id, end_id, entry_id, exit_id)):
            raise ValueError(f"Migration identity collision: {group.group_id}")
        by_node = {n["id"]: n for n in nodes}
        if group.source_id not in by_node or group.join_id not in by_node:
            raise ValueError("Parallel source and convergence must exist")
        selected = [e for e in edges if e["id"] in group.branch_edge_ids]
        if len(selected) != len(set(group.branch_edge_ids)) or len(selected) < 2:
            raise ValueError("Parallel migration requires at least two known branch arrows")
        if any(e["source"] != group.source_id or e.get("backward") for e in selected):
            raise ValueError("Parallel branches must share a forward source")
        outgoing: dict[str, list[dict[str, Any]]] = {}
        for edge in edges:
            if not edge.get("backward"):
                outgoing.setdefault(edge["source"], []).append(edge)
        region: set[str] = set()
        arrivals: set[str] = set()
        for branch in selected:
            pending = [branch["target"]]
            visited: set[str] = set()
            while pending:
                current = pending.pop()
                if current == group.join_id:
                    raise ValueError("Parallel branch must contain a node before convergence")
                if current in visited:
                    continue
                visited.add(current)
                if current not in by_node or by_node[current]["type"] == "end":
                    raise ValueError("Parallel branch escapes before convergence")
                next_edges = outgoing.get(current, [])
                if not next_edges:
                    raise ValueError("Parallel branch does not reach convergence")
                for edge in next_edges:
                    if edge["target"] == group.join_id:
                        arrivals.add(edge["id"])
                    else:
                        pending.append(edge["target"])
            if region & visited:
                raise ValueError("Parallel branches overlap before convergence")
            region |= visited
        allowed_entries = set(group.branch_edge_ids)
        if any(e["target"] in region and e["source"] not in region and e["id"] not in allowed_entries for e in edges):
            raise ValueError("External arrow enters a parallel branch")
        source_pos = by_node[group.source_id].get("position", {"x": 0, "y": 0})
        join_pos = by_node[group.join_id].get("position", {"x": 0, "y": 0})
        for nid, kind, position in (
            (start_id, "parallel_start", source_pos),
            (end_id, "parallel_end", join_pos),
        ):
            nodes.append({"id": nid, "type": kind, "parallel_group_id": group.group_id,
                          "title": group.title, "position": {"x": max(0, position["x"]), "y": max(0, position["y"] - 120)}})
        for edge in selected:
            edge["source"] = start_id
        for edge in edges:
            if edge["id"] in arrivals:
                edge["target"] = end_id
        edges.extend([
            {"id": entry_id, "source": group.source_id, "target": start_id, "condition": group.condition},
            {"id": exit_id, "source": end_id, "target": group.join_id, "condition": "All parallel branches have resolved; continue with their combined results."},
        ])
    converted["routing_mode"] = "explicit"
    return converted
