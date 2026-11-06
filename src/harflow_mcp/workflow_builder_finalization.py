"""Ordinary builder finalization serialized with explicit steering admission."""
from __future__ import annotations

import copy
from types import SimpleNamespace


def finish_ordinary_builder(supervisor, activation, result):
    """The tree lock is the admission-versus-autosave linearization boundary."""
    from . import store as task_store
    from .workflow_steering import held
    from .workflows import parse_json, validate_definition
    self, a = supervisor, activation
    with self.store.tree_mutation(self.run_id):
        current = self.run(refresh_atomic=True)
        if current["status"] != "running":
            return
        from .conversation_workflows import _bindings, guard_outcome
        if a['id'] in _bindings(current):
            choice=[]
            self.update(lambda run: choice.append(guard_outcome(run,a['id'],result)), 'builder_conversation_result')
            if choice != ['accept']:
                return
        if held(current):
            def superseded(run):
                old = next(item for item in run["activations"] if item["id"] == a["id"])
                old["steering_superseded"] = True
                run.update(status="paused", attention_reason="Builder settled; queued steering owns the next turn")
            self.update(superseded, "builder_final_superseded_by_steering")
            return
        try:
            current = self.run()
            applied = next(x for x in current["activations"] if x["id"] == a["id"]).get("draft_applied", False)
            proposal = copy.deepcopy(current["builder_draft"] if applied else parse_json(result.get("summary") or ""))
            baseline = current.get("editing_definition", current.get("generated_definition", {}))
            previous_nodes = {n["id"]: n for n in baseline.get("nodes", [])}
            for proposed_node in proposal.get("nodes", []):
                if proposed_node.get("type") == "agent" and "execution_mode" not in proposed_node:
                    proposed_node["execution_mode"] = previous_nodes.get(proposed_node["id"], {}).get("execution_mode", "headless") if proposed_node["id"] in previous_nodes else "prefer_subagent"
            d = validate_definition({**proposal, "name": current["name"], "routing_mode": "explicit"})
            d["draft"] = True
            if "editing_definition" in self.run() or self.run().get("builder_followup"):
                metadata = self.run().get("editing_source", {"name": self.run()["name"], "revision": self.run().get("generated_definition", {}).get("revision", 0)})
                d["revision"] = metadata["revision"] if metadata["name"] == d["name"] else 0
                d.pop("updated_at", None)
                self.update(lambda r: (r.update(status="completed", generated_definition=d, builder_draft=d, draft_revision=r.get("draft_revision", 0) + 1), next(x for x in r["activations"] if x["id"] == a["id"]).update(status="completed")), "builder_edit_proposed")
            else:
                if current.get("caller_record"):
                    from .workflow_inspection import guard_saved_workflow_authority
                    record = task_store.TaskRecord(**current["caller_record"])
                    # The guard sees the resolved dependency tree before any write.
                    saved = self.store.save(d["name"], d, authority_guard=lambda tree: guard_saved_workflow_authority(SimpleNamespace(record=record), tree))
                else:
                    saved = self.store.save(d["name"], d)
                self.update(lambda r: (r.update(status="completed", generated_definition=saved, builder_draft=saved, draft_revision=r.get("draft_revision", 0) + 1), next(x for x in r["activations"] if x["id"] == a["id"]).update(status="completed")), "builder_saved")
        except (ValueError, FileExistsError) as exc:
            self.attention(f"Workflow proposal was invalid: {exc}" if "editing_definition" in self.run() else f"Generated workflow was not saved: {exc}")


def pending_steering_handoff(run, observed_turn_ids):
    """Wake a newly admitted request after releasing the old supervisor's flock.

    The request already owns its issuance. Never loop an old unknown/failed owner;
    an explicit same-request observation can recover those without another effect.
    """
    return any(
        turn["turn_id"] not in observed_turn_ids and turn["state"] in {"queued", "reserved", "captured"}
        for turn in run.get("steering", {}).get("turns", [])
    )
