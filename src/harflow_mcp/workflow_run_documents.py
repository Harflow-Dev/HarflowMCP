"""Pure qualification of persisted workflow run documents.

Authored snapshots remain JSON values; qualification never executes or migrates a graph.
"""
from __future__ import annotations

import copy
from datetime import datetime
import math
import re
from typing import Any

from .wire_contracts import PRODUCT, PRODUCT_VERSION, compatible_line

RUN_FORMAT = "harflow.workflow.run"
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}")


class WorkflowRunDocumentError(Exception):
    """An existing source cannot provide qualified workflow authority."""

    def __init__(self, run_id: str, reason: str, *, code: str = "workflow_run_incompatible", published: bool = False):
        self.run_id, self.reason, self.code, self.published = run_id, reason, code, published
        super().__init__(f"{run_id}: {reason}")

    def as_dict(self) -> dict[str, Any]:
        result = {"workflow_run_id": self.run_id, "code": self.code, "reason": self.reason}
        if self.published:
            result["published"] = True
        diagnostic = getattr(self, "receipt_diagnostic", None)
        if diagnostic is not None:
            result["receipt_diagnostic"] = copy.deepcopy(diagnostic)
        return result


def identifier(value: Any) -> str:
    if type(value) is not str or not _IDENTIFIER.fullmatch(value):
        raise ValueError("Invalid workflow run identifier")
    return value


def _json(value: Any) -> None:
    if value is None or type(value) in (str, bool, int):
        return
    if type(value) is float and math.isfinite(value):
        return
    if type(value) is list:
        for item in value:
            _json(item)
        return
    if type(value) is dict and all(type(key) is str for key in value):
        for item in value.values():
            _json(item)
        return
    raise ValueError("Run must contain finite JSON values")


# Schema descriptors validate supplied fields and preserve additive members.
# Requiredness is deliberately confined to the reviewed common traversal core.
N = type(None)
NUMBER = (int, float)
NULL_STRING = (str, N)
NULL_OBJECT = (dict, N)
NULL_NUMBER = (int, float, N)


def _shape(value: Any, fields: dict, path: str) -> None:
    if type(value) is not dict:
        raise ValueError(f"Malformed {path}: expected object")
    for key, check in fields.items():
        if key not in value:
            continue
        item = value[key]
        where = f"{path}.{key}"
        if callable(check) and not isinstance(check, type):
            check(item, where)
        elif type(item) not in (check if type(check) is tuple else (check,)):
            raise ValueError(f"Malformed {where}")
        elif type(item) is float and not math.isfinite(item):
            raise ValueError(f"Nonfinite {where}")


def _counter(value: Any, path: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"Malformed {path}: expected nonnegative integer")


def _positive(value: Any, path: str) -> None:
    _counter(value, path)
    if value == 0:
        raise ValueError(f"Malformed {path}: expected positive integer")


def _nullable(check):
    def validate(value, path):
        if value is not None:
            check(value, path)
    return validate


def _array(check):
    def validate(value, path):
        if type(value) is not list:
            raise ValueError(f"Malformed {path}: expected array")
        for index, item in enumerate(value):
            if isinstance(check, type):
                if type(item) is not check:
                    raise ValueError(f"Malformed {path}[{index}]")
            else:
                check(item, f"{path}[{index}]")
    return validate


def _map(check):
    def validate(value, path):
        if type(value) is not dict:
            raise ValueError(f"Malformed {path}: expected object")
        for key, item in value.items():
            if isinstance(check, type):
                if type(item) is not check:
                    raise ValueError(f"Malformed {path}.{key}")
            else:
                check(item, f"{path}.{key}")
    return validate


def _object(fields):
    return lambda value, path: _shape(value, fields, path)


def _id(value, path):
    identifier(value)


def _fields(strings="", counters="", numbers="", booleans="", objects="", arrays="", nullable_strings=""):
    result = {key: str for key in strings.split()}
    result.update({key: _counter for key in counters.split()})
    result.update({key: NUMBER for key in numbers.split()})
    result.update({key: bool for key in booleans.split()})
    result.update({key: dict for key in objects.split()})
    result.update({key: list for key in arrays.split()})
    result.update({key: NULL_STRING for key in nullable_strings.split()})
    return result


def _candidate(value, path):
    _shape(value, {**_fields("backend instructions", booleans="caller_approvals", nullable_strings="model reasoning_effort"),
                   "max_turns": _nullable(_positive), "background_idle_seconds": NULL_NUMBER, "fallbacks": _array(_candidate)}, path)


def _identity(value, path):
    _shape(value, {"pid": _positive, "start_time": NULL_STRING, "markers": _array(str)}, path)
    missing = {"pid", "start_time", "markers"} - value.keys()
    if missing:
        raise ValueError(f"Incomplete {path}: missing {', '.join(sorted(missing))}")


def _binding(value, path):
    _shape(value, {"candidate": str, "task_id": _id, "session_id": NULL_STRING}, path)


def _selection(value, path):
    _shape(value, {**_fields("requested_policy selected_mode reason session_ref compatibility_fingerprint source_execution_id source_child_workflow_run_id source_task_id source_session_id"),
                   "candidate": _candidate, "binding": _binding}, path)
    if "source_child_workflow_run_id" in value:
        identifier(value["source_child_workflow_run_id"])


def _delivery(value, path):
    _shape(value, {"id": str, "payload": _object({**_fields("source_id action", counters="additional_attempts", booleans="allow_optional_review_skip", nullable_strings="instructions decision_id interaction_owner"), "source": _source})}, path)


def _source(value, path):
    _shape(value, {"workflow_run_id": _id, "workflow_name": str, "path": _array(str), "attention_checkpoint": _counter}, path)
    for item in value.get("path", []):
        identifier(item)


def _inspection(value, path):
    _shape(value, {**_fields("view execution_id node_id", nullable_strings="task_id workflow_run_id cursor"), "limit": _nullable(_counter), "before_seq": _nullable(_counter), "after_seq": _nullable(_counter)}, path)


def _question(value, path):
    _shape(value, {**_fields("question_id question origin_task_id status decision_id answer_delivery_state raw_output answer reply_task_id session_mode", counters="decision_attempts inspection_count on_demand_count", numbers="created_at delivered_at answered_at", arrays="evidence", objects="progress"),
                   "inspection_results": _array(_inspection_result), "origin_candidate": _candidate, "child_question": _object({"workflow_run_id": _id, "decision_id": str, "question": str}), "child_answer_delivery": _delivery}, path)


def _dispatch(value, path):
    _shape(value, {**_fields("assignment_prompt prompt continuation_id candidate_id selection_reason reason session_mode session_ref session_reason child_session_mode child_session_ref child_session_reason resume_task_id", booleans="continue_previous requires_assignment", arrays="assigned_task_ids input_result_refs additional_result_refs authorized_artifact_refs"),
                   "authorized_artifact_refs": _array(_artifact), "authorized_input_refs": _array(_input_ref), "structural_dispatch": _dispatch, "child_session_selection": _selection, "branch_assignments": _array(_dispatch), "artifact_refs": _array(_object(_fields("execution_id artifact_id")))}, path)


def _token(value, path):
    _shape(value, {**_fields("id node_id via assignment_prompt decision_id decision_error accepted_decision_id execution_activation_id completion_source_node_id retry_of_execution_id reopen_child_execution_id resume_task_id resume_source_execution_id execution_session_mode session_reason child_session_mode child_session_ref child_session_reason recovery_execution_id recovery_node_id", counters="decision_attempts inspection_count on_demand_count", booleans="requires_assignment execution_complete continue_previous joined", arrays="inspection_results"),
                   **{key: _array(str) for key in "stack failed_execution_refs input_result_refs additional_result_refs assigned_task_ids completed_task_ids selected_connections recovered_execution_refs structural_completion_refs".split()},
                   "optional_failure_join": str, "result": _pending_result, "selected_join_id": NULL_STRING, "branch_ids": _map(str), "assignments": _map(_dispatch), "structural_dispatch": _dispatch, "recovery_return_checkpoint": _token, "context": dict, "inspection_results": _array(_inspection_result), "recovered_result": _result, "recovered_failed_result": _result, "authorized_artifact_refs": _array(_artifact), "authorized_input_refs": _array(_input_ref)}, path)


def _join(value, path):
    _shape(value, {**_fields("join_id split_id selection_reason", counters="expected selection_sequence", nullable_strings="selection_decision_id"),
                   **{key: _array(str) for key in "stack selected_targets branch_ids arrival_ids selected_connection_ids excluded_connection_ids".split()},
                   "arrived": _array(_token), "parent_branch_ids": _map(str), "branch_states": _map(str), "branch_assignments": _map(_dispatch)}, path)


def _diagnostic(value, path):
    _shape(value, {"category": str, "reason": str, "source": str, "reset_at": (str, int, float)}, path)
    if "reset_at" in value:
        reset = value["reset_at"]
        if type(reset) is str:
            if len(reset) > 100 or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)", reset):
                raise ValueError(f"Malformed {path}.reset_at")
            if datetime.fromisoformat(reset.replace("Z", "+00:00")).utcoffset() is None:
                raise ValueError(f"Malformed {path}.reset_at")
        else:
            try:
                finite = math.isfinite(reset)
            except OverflowError:
                finite = False
            if not finite:
                raise ValueError(f"Malformed {path}.reset_at")


def _snapshot_authority(value, path):
    from types import SimpleNamespace
    from .task_documents import _nested, _shape as task_shape, _grants, qualify_enforcement
    task_shape(value, {"enforcement": dict[str, Any] | None, "owner": dict[str, Any] | None, "outcome": dict[str, Any] | None, "git_baseline": dict[str, Any] | None}, path)
    _nested(SimpleNamespace(git_baseline=value.get("git_baseline"), failure_diagnostic=value.get("failure_diagnostic"), outcome=value.get("outcome")))
    if value.get("owner") is not None:
        _identity(value["owner"], path + ".owner")
    enforcement = value.get("enforcement")
    if enforcement is not None:
        qualify_enforcement(enforcement, path=path + ".enforcement")
    _grants(value.get("extra_writable_paths", []))


def _followup_view(value, path):
    item = {**_fields("id status text successor_task_id reason at queued_at", counters="seq")}
    _shape(value, {"state": str, "items": _array(_object(item)), "omitted_final": _counter,
                   "history_incomplete": bool,
                   "refused_tasks": _array(_object(_fields("task_id code reason")))}, path)


def _result(value, path):
    _shape(value, {**_fields("status backend task_id repo_path freedom started_at raw_stream_log events_log interaction_owner recovery_reason failure_kind execution_failure active_model provider approval_policy parent_session_id cli_version settings_source model_provider cleanup_source", counters="depth auto_continue", numbers="duration_seconds", booleans="native_terminal can_cancel_child can_resume_child network_access output_drained metadata_persisted caller_approvals workflow_builder live_input prompt_truncated recovered taken_over outcome_unknown timed_out blocked_failure optional_failure_eligible", nullable_strings="session_id model reasoning_effort summary parent_task_id spawned_by root_task_id continuation_of_task_id chain_root_task_id group title base_commit resume_command lineage_detected extra_path_certification prompt_source prompt_error note taken_over_note start_time owner_session_id native_child_id observed_model"),
                   **{key: _array(str) for key in "notices stderr_tail last_output_tail markers denials bridge_notices".split()},
                   **{key: (dict, N) for key in "usage failure_diagnostic enforcement owner outcome approval_summary artifact_summary persistence_error git_baseline".split()},
                   **{key: _array(dict) for key in "permission_denials mcp_servers pending_messages extra_writable_paths".split()},
                   **{key: (bool, N) for key in "owned_by_live_server is_error start_dirty network".split()},
                   **{key: _nullable(_counter) for key in "num_turns available_tool_count max_depth".split()},
                   "max_turns": _nullable(_positive), "exit_code": (int, N), "total_cost_usd": NULL_NUMBER, "timeout_seconds": NULL_NUMBER, "queued_followups": _followup_view, "queued_followup_count": _nullable(_counter),
                   "failure_diagnostic": _nullable(_diagnostic), "candidates": _array(_candidate), "sandbox_policy": _object({"type": str, "network_access": bool, "writable_roots": _array(str), "readable_roots": _array(str)}),
                   "pid": _nullable(_positive), "pgid": _nullable(_positive)}, path)
    _snapshot_authority(value, path)


def _context(value, path):
    receipt = {**_fields("scope digest bootstrap_digest delivery_mode mode owner_prefix compatibility_reason", counters="version revision bytes characters budget prompt_version serialized_bytes serialized_characters total_bytes total_characters budget_bytes budget_overflow_bytes", booleans="acknowledged", nullable_strings="session_owner checkpoint"), "base_revision": _nullable(_counter)}
    _shape(value, {**receipt, "receipt": _object(receipt), "ack": _object(receipt), "accounting": _object(receipt), "sections": _map(_object({"bytes": _counter, "characters": _counter})), "context_fields": _map(_object({"bytes": _counter, "characters": _counter})), "section_state": _map(_object({**_fields("digest mode", counters="revision source_bytes source_characters delivered_bytes delivered_characters length")})), "section_delivery": _map(_object({**_fields("digest mode", counters="revision source_bytes source_characters delivered_bytes delivered_characters length")})), "compatibility_reasons": _array(str), "role": str, "session_mode": str, "classification": str}, path)


def _artifact(value, path):
    _shape(value, {**_fields("execution_id artifact_id owner_task_id workflow_run_id node_id kind path note availability_at_assignment attribution", numbers="registered_at", booleans="inherited"), "registered_observation": dict}, path)
    for key in ("owner_task_id", "workflow_run_id"):
        if key in value:
            identifier(value[key])


def _inspection_result(value, path):
    _shape(value, {"request": _inspection, "response": _object({**_fields("workflow_run_id execution_id node_id status encoding content_sha256 chunk", counters="offset total_characters", booleans="has_more", nullable_strings="task_id next_cursor"), "attempts": _array(_object({"task_id": NULL_STRING, "status": NULL_STRING}))}), "summary": str}, path)


def _authored(value, path):
    from .workflow_definition_documents import _shape as definition_shape, _candidate as definition_candidate
    definition_shape(value, {"name": str, "workflow_id": str, "nodes": list, "connections": list, "orchestrator": dict, "on_demand_nodes": list}, path)
    if "orchestrator" in value:
        definition_candidate(value["orchestrator"])
    for node in value.get("nodes", []) + value.get("on_demand_nodes", []):
        if type(node) is not dict:
            raise ValueError(f"Malformed {path}.node")
        definition_shape(node, {"id": str, "type": str, "agent": dict, "freedom": str, "network": (bool, N), "workflow_ref": dict}, path + ".node")
        if "agent" in node:
            definition_candidate(node["agent"])
    for edge in value.get("connections", []):
        if type(edge) is not dict:
            raise ValueError(f"Malformed {path}.connection")


def _attempt(value, path):
    _shape(value, {**_fields("task_id status dispatch_stage freedom repo_path session_mode assignment_prompt execution_kind dispatch_nonce owner_task_id owner_session_id transport_task_id assignment_sha256 activity_level native_owner_execution_id error recovery_reason", numbers="reserved_at finished_at started_at timeout_deadline timeout_requested_at", booleans="usage_recovery_checkpointed native_terminal can_cancel_child can_resume_child context_acknowledged", nullable_strings="resume_task_id execution_fallback_reason native_child_id owner_session_generation"),
                   "candidate": _candidate, "network": (bool, N), "result": _result, "context_delivery": _context, "prompt_usage": _object({"usage": NULL_OBJECT, "cost_usd": NULL_NUMBER}), "liveness": _object({"process_alive": (bool, N), "outcome_known": bool, "resolved_status": str, "reason": str}), "native_owner_execution_ids": _array(str),
                   "harness_metadata": _object({"requested": _candidate, "effective": _object({**_fields("backend freedom settings_source provider_identity", nullable_strings="model reasoning_effort default_model"), "network": (bool, N)}), "observed": _nullable(_result), "verification_status": str, "provenance": str, "metadata_source": str})}, path)
    missing = {"task_id", "status"} - value.keys()
    if missing:
        raise ValueError(f"Incomplete {path}: missing {', '.join(sorted(missing))}")
    identifier(value["task_id"])


def _invocation(value, path):
    _shape(value, {**_fields("stage workflow_id workflow_name orchestrator_mode definition_sha256 assignment child_session_refusal timeout_reason", numbers="timeout_elapsed_seconds timeout_started_at settled_at started_at finished_at active_since", booleans="timeout_paused timeout_expired timeout_confirmed", arrays="inputs artifact_refs"),
                   "child_workflow_run_id": _id, "revision": _nullable(_counter), "timeout_seconds": NULL_NUMBER, "child_session_selection": _selection, "inputs": _array(_invocation_input), "artifact_refs": _array(_artifact)}, path)


def _node_result(value, path):
    _shape(value, {"status": str, "evidence": list, "result": _object({**_fields("technical_plan checklist_reason verdict question failure_kind reason blocker_category", booleans="no_checklist_needed"), "tasks": _array(_object(_fields("id title description"))), "completed_task_ids": _array(str), "child_outcome": _child_outcome})}, path)


def _pending_result(value, path):
    """Qualify normalized outcomes and legacy harness/optional-failure projections."""
    _result(value, path)
    _node_result(value, path)
    _shape(value, {"optional_failure": str, "failure_evidence": _result}, path)


def _activation(value, path):
    _shape(value, {**_fields("id node_id role status visit_id decision_id assignment_prompt raw_output result_error pending_question_id resume_question_id turn_prompt retry_of_execution_id resolved_by_execution_id execution_kind on_demand_call_id execution_session_mode owner_task_id activity_level", numbers="created_at finished_at", arrays="resolved_execution_refs input_result_refs authorized_artifact_refs feedback_ids inspection_results", nullable_strings="execution_fallback_reason"),
                   "optional_failure": bool, "assigned_task_ids": _array(str), "resolved_execution_refs": _array(str), "input_result_refs": _array(str), "token": _nullable(_token), "tasks": _array(_attempt), "questions": _array(_question), "result": dict, "node_result": _node_result, "invocation": _invocation, "inspection_requests": _array(_inspection), "can_cancel_child": bool, "can_resume_child": bool, "native_child_id": NULL_STRING, "draft_applied": bool, "continue_previous": bool, "authorized_artifact_refs": _array(_artifact), "authorized_input_refs": _array(_input_ref), "inspection_results": _array(_inspection_result), "resume_task_id": NULL_STRING, "on_demand_node": dict, "context_delivery": _context}, path)
    missing = {"id", "node_id", "role", "status", "created_at", "tasks"} - value.keys()
    if missing:
        raise ValueError(f"Incomplete {path}: missing {', '.join(sorted(missing))}")


def _decision(value, path):
    _shape(value, {**_fields("node_id activation_id action decision_id reason question execution_id question_id answer session_mode actor token_id", arrays="requests protocol_warnings selected_connections", counters="transition_sequence", nullable_strings="selected_join_id"), "connections": _array(str), "next": _array(_dispatch), "task_updates": _array(_object(_fields("task_id status reason")))}, path)


def _point(value, path):
    _shape(value, {**_fields("key event status snapshot_source", counters="sequence", numbers="observed_at", booleans="snapshot_error snapshot_evicted requires_tree_check"),
                   "snapshot": _nullable(_object({**_fields("status name kind"), "goal_revision": _nullable(_counter), "definition": _object({"workflow_id": NULL_STRING, "revision": _nullable(_counter), "definition_hash": NULL_STRING}),
                       "attention": _nullable(_object({**_fields("kind question reason", booleans="question_truncated", nullable_strings="input_decision_id"), "source": _nullable(_object({**_fields("workflow_run_id workflow_name", counters="attention_checkpoint"), "path": _array(str)}))})),
                       "result": _nullable(_object({**_fields("summary failure_reason"), "checklist_disposition": _nullable(_object(_fields("status execution_id decision_id reason")))}))}))}, path)


def _owner_contracts(value, path):
    node = {**_fields("workflow_id node_id title freedom execution_kind", counters="candidate_position", nullable_strings="fallback_reason approval_scope approval_transport"), "network": (bool, N), "candidate": _candidate, "caller_approvals": bool}
    plan = {"candidate": _candidate, "freedom": str, "network": (bool, N), "nodes": _array(_object(node)), "contributing_nodes": _array(_object(node)), "caller_approvals": bool, "approval_scope": NULL_STRING, "approval_transport": NULL_STRING}
    _shape(value, {**_fields("version root_workflow_id contract_hash"), "owners": _map(_object({**_fields("workflow_id name"), "candidates": _map(_object(plan))}))}, path)


def _dependency(value, path):
    _shape(value, {"root_workflow_id": str, "workflows": _map(_object({**_fields("name definition_sha256", counters="revision"), "definition": _authored})), "edges": _array(_object(_fields("from node_id to orchestrator_mode"))), "access": _map(_object({"max_freedom": str, "network": bool, "backends": _array(str), "depth": _counter}))}, path)


def _recovery(value, path):
    _shape(value, {**_fields("decision_id role node_id execution_id accepted_answer selection_failure", numbers="accepted_at", nullable_strings="token_id"), "failure_diagnostic": _diagnostic, "failed_candidate": _candidate, "choices": _array(_object({"answer": str, "candidate": _candidate}))}, path)


CORE = _fields("workflow_run_id kind name definition_hash prompt repo_path freedom status instructions permission_policy", counters="revision sequence transitions", numbers="created_at updated_at")
CORE.update(definition=_authored, network=(bool, N), activations=_array(_activation), decisions=_array(_decision), tasks=_array(_object(_fields("id title description status source_activation_id disposition reason disposition_execution_id disposition_decision_id status_decision_activation_id", numbers="status_changed_at", nullable_strings="completed_by_activation_id"))), sessions=_map(_binding), pending=_array(_token), joins=_map(_join), attempt_grants=_map(_counter), retry_counts=_map(_counter), retry_grants=_map(_counter), suppressed_candidates=_array(str), supervisor_pid=_nullable(_positive))
OPTIONAL = _fields("scheduling_policy execution_contract runner_policy execution_policy interaction_owner reason wait_reason summary failure_reason attention_reason input_question input_decision_id technical_plan technical_plan_execution_id workflow_id orchestrator_mode caller_method goal source_name failed_since_source pause_origin failed_decision_id", counters="transition_grant goal_revision goal_replanning_revision source_revision draft_revision attention_checkpoint failed_since_sequence notification_point_error", booleans="execution_initialized recovered_inherited_turn suspended_via_root optional_review_skip_available goal_replanning_required", objects="builder_draft generated_definition editing_definition checklist_disposition released_parallel_groups context_delivery context_ack")
OPTIONAL.update(supervisor_identity=_nullable(_identity), owner_contracts=_nullable(_owner_contracts), dependency_tree=_dependency, parent_link=_object({"workflow_run_id": _id, "root_workflow_run_id": _id, "execution_id": str, "node_id": str, "depth": _counter}), orchestrator_session_owner_run_id=_id, orchestrator_config=_candidate, parent_goal=_object({"workflow_run_id": _id, "goal": str, "revision": _counter}), child_session_selection=_selection, inherited_orchestrator_binding=_selection, invocation_inputs=list, invocation_artifact_refs=list, forwarded_delivery=_delivery, forwarded_delivery_receipt=_delivery, input_source=_source, attention_source=_source, orchestrator_usage_recovery=_recovery, worker_usage_recovery=_recovery, usage_recovery_history=_array(_recovery), orchestrator_recovery_candidate=_candidate, goal_history=_array(_object({"revision": _counter, "goal": str, "created_at": NUMBER})), goal_assessment=_nullable(_object({"revision": _counter, "status": str, "reason": str, "evidence_refs": _array(str)})), goal_replan=_object({"revision": _counter, "plan": str}), editing_source=_object({"name": str, "revision": _counter, "saved_definition": NULL_OBJECT}), source_saved_definition=NULL_OBJECT, notification_points=_array(_point), terminal_notification_points=_array(_point), notification_evicted=_object({"attention": _counter, "terminal": _counter}))
for _key in ("suppressed_capability_candidates", "exhausted_retry_edges"):
    OPTIONAL[_key] = _array(str)
for _key in ("optional_skip_authorizations", "blocker_retry_authorizations", "protocol_retry_authorizations"):
    OPTIONAL[_key] = _map(_object({**_fields("node_id reason source_decision_id authorization_kind", numbers="granted_at", booleans="allow_optional_review_skip")}))


OPTIONAL.update(released_parallel_groups=_map(_object({**_fields("join_id merged_token_id", counters="expected", nullable_strings="split_id"), "branch_states": _map(str), "branch_ids": _array(str), "branch_assignments": _map(_dispatch), "parent_branch_ids": _map(str), "stack": _array(str), "selection_sequence": _counter, "selection_reason": str, "selection_decision_id": NULL_STRING, "selected_connection_ids": _array(str), "excluded_connection_ids": _array(str)})), context_delivery=_context, context_ack=_context)
OPTIONAL["on_demand_calls"] = _array(_object({**_fields("id node_id source_workflow_run_id local_node_id title assignment reason status execution_id orchestration_activation_id dispatch_state execution_session_mode", counters="call_number before_count", booleans="result_delivered", nullable_strings="task_id failure resume_task_id"), "started_at": NULL_NUMBER, "finished_at": NULL_NUMBER, "result": _nullable(_node_result), "action": _decision, "result_ref": _object({"workflow_run_id": _id, "execution_id": str}), "checkpoint": _object({"workflow_run_id": _id, "decision_id": str, "token_id": str, "question_id": NULL_STRING}), "origin": _object(_fields("kind node_id title workflow_run_id"))}))


def _input_result(value, path):
    _shape(value, {**_fields("result_ref execution_id node_id status", counters="attempt", booleans="retry_eligible omitted", nullable_strings="role"), "workflow_run_id": _id, "result": dict, "node_result": _node_result, "child_outcome": _object({"workflow_run_id": _id, "status": str, "final_result_refs": _array(_object({"workflow_run_id": _id, "execution_id": str}))})}, path)


def _invocation_input(value, path):
    _shape(value, {"index": _positive, "kind": str, "results": _array(_input_result), "descriptors": _array(_object(_fields("id title description")))}, path)


OPTIONAL.update(invocation_inputs=_array(_invocation_input), invocation_artifact_refs=_array(_artifact), checklist_disposition=_object(_fields("status reason execution_id decision_id")))


def _input_ref(value, path):
    _shape(value, {"workflow_run_id": _id, "execution_id": str, "task_id": _nullable(_id), "content_sha256": str, "total_characters": _counter, "encoding": str}, path)


def _child_outcome(value, path):
    _shape(value, {**_fields("kind workflow_name orchestrator_mode summary failure_reason child_session_refusal", booleans="summary_truncated", nullable_strings="child_status workflow_id definition_sha256"), "child_workflow_run_id": _nullable(_id), "revision": _nullable(_counter), "final_result_refs": _array(_object({"workflow_run_id": _id, "execution_id": str, "node_id": NULL_STRING, "via": _array(_id)})), "checklist_summary": _array(_object({"id": NULL_STRING, "title": NULL_STRING, "status": NULL_STRING})), "permission_evidence": _array(dict), "child_session_selection": _selection}, path)


OPTIONAL.update(builder_has_repo_context=bool, builder_followup=bool, builder_turn_prompt=str, builder_turn_feedback_ids=_array(str), builder_messages=_array(_object({**_fields("id prompt status inbox_message_id task_id", numbers="created_at"), "inbox_message_id": NULL_STRING})))


def qualify_payload(value: Any, run_id: str) -> dict[str, Any]:
    """Validate inner authority without applying graph or runtime semantics."""
    try:
        identifier(run_id)
        _json(value)
        if type(value) is not dict:
            raise ValueError("Workflow run core must be an object")
        missing = CORE.keys() - value.keys()
        if missing:
            raise ValueError(f"Incomplete workflow run core: missing {', '.join(sorted(missing))}")
        if value["workflow_run_id"] != run_id:
            raise ValueError("Workflow run id does not match source id")
        _shape(value, {**CORE, **OPTIONAL}, "run")
        if "caller_record" in value:
            from .task_documents import qualify_payload as qualify_task_payload
            from .store import TaskRecord
            caller = value["caller_record"]
            if type(caller) is not dict:
                raise ValueError("Malformed run.caller_record")
            identifier(caller.get("task_id"))
            for key in ("parent_task_id", "continuation_of_task_id", "chain_root_task_id", "spawned_by", "root_task_id"):
                if caller.get(key) is not None:
                    identifier(caller[key])
            qualify_task_payload(caller, caller.get("task_id"), TaskRecord)
        return copy.deepcopy(value)
    except WorkflowRunDocumentError:
        raise
    except Exception as exc:
        from .task_documents import TaskDocumentError
        if not isinstance(exc, (TypeError, ValueError, RecursionError, TaskDocumentError)):
            raise
        raise WorkflowRunDocumentError(run_id, str(exc)) from exc


def decode(document: Any, run_id: str) -> dict[str, Any]:
    """Return the qualified inner run, preserving unknown additive JSON values."""
    if type(document) is not dict or document.get("product") != PRODUCT or document.get("format") != RUN_FORMAT:
        raise WorkflowRunDocumentError(run_id, "Missing or foreign workflow run product/format")
    if type(document.get("v")) is not int or document["v"] != 1:
        raise WorkflowRunDocumentError(run_id, "Unsupported workflow run document version")
    try:
        compatible = compatible_line(document.get("product_version"))
    except (TypeError, ValueError) as exc:
        raise WorkflowRunDocumentError(run_id, str(exc)) from exc
    if not compatible:
        raise WorkflowRunDocumentError(run_id, "Incompatible workflow run product major/minor line")
    return qualify_payload(document.get("run"), run_id)


def encode(run: dict[str, Any]) -> dict[str, Any]:
    """Validate the complete candidate before producing an envelope."""
    run_id = run.get("workflow_run_id") if type(run) is dict else None
    value = qualify_payload(run, run_id)
    return {"product": PRODUCT, "format": RUN_FORMAT, "v": 1, "product_version": PRODUCT_VERSION, "run": value}
