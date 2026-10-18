"""The shared wait contract: wait_for_task, wait_for_tasks and wait_for_workflow. No model calls."""
import asyncio
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from mcp import MCPError

from polybridge import ctl, identity, server, store, task_waiting, wait_policy
from polybridge.tasks import Task

SCALE = 1000.0


@pytest.fixture
def fast_clock(monkeypatch):
    """Waits measure fake seconds 1000x faster than real ones; slices stay a few real ms."""
    monkeypatch.setattr(wait_policy, "monotonic", lambda: time.monotonic() * SCALE)
    monkeypatch.setattr(wait_policy, "PROGRESS_INTERVAL_SECONDS", 0.005)
    monkeypatch.setattr(server, "WORKFLOW_POLL_SECONDS", 0.005)


class Ctx:
    def __init__(self, fail=False):
        self.reports = []
        self.fail = fail

    async def report_progress(self, progress, total=None, message=None):
        self.reports.append((progress, total, message))
        if self.fail:
            raise RuntimeError("client rejects progress")


def add_task(task_id, tmp_path, status="running"):
    task = Task(task_id=task_id, backend="codex", session_id=None, repo_path=tmp_path,
                prompt="x", max_turns=None, log_path=tmp_path / f"{task_id}.jsonl",
                started_at=datetime.now(timezone.utc), status=status)
    if status != "running":
        task.done.set()
    server._reg()._tasks[task_id] = task
    return task


@pytest.fixture(autouse=True)
def workflow_run_file(tmp_path):
    """The run file wait_for_workflow watches, under the registry's root (as retention sees it)."""
    directory = server._reg().log_dir.parent / "workflow-runs"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "r.json"
    path.write_text("{}")
    return path


@pytest.fixture
def fake_clock(monkeypatch):
    """A deterministic clock for task_waiting: sleeping advances it, nothing really waits."""
    clock = [100.0]
    async def sleep(interval):
        clock[0] += max(interval, 0.001)
    monkeypatch.setattr(task_waiting, "_now", lambda: clock[0])
    monkeypatch.setattr(task_waiting, "_sleep", sleep)
    monkeypatch.setattr(task_waiting, "POLL_SECONDS", 1.0)
    return clock


def write_record(tmp_path, task_id="rec", **overrides):
    fields = dict(task_id=task_id, backend="claude", session_id="s", markers=["s"],
                  repo_path=str(tmp_path), started_at=datetime.now(timezone.utc).isoformat(),
                  pid=4321, status="failed", exit_code=None)
    fields.update(overrides)
    record = store.TaskRecord(**fields)
    store.write(server._reg().log_dir, record)
    return record


# --- validation -------------------------------------------------------------------------------

@pytest.mark.parametrize("timeout,until", [
    (3601, "timeout"), (-1, "timeout"), (True, "timeout"), (1.5, "timeout"), ("5", "timeout"),
    (5, "terminal"), (0, "terminal"), (None, "bogus"),
])
def test_plan_rejects_invalid_requests(timeout, until):
    with pytest.raises(ValueError):
        wait_policy.WaitPlan.validate(timeout, until, 30)


def test_plan_honours_requests_up_to_the_declared_maximum():
    assert wait_policy.WaitPlan.validate(3600, "timeout", 30).effective == 3600
    assert wait_policy.WaitPlan.validate(0, "timeout", 30).effective == 0
    assert wait_policy.WaitPlan.validate(None, "timeout", 55).requested == 55
    plan = wait_policy.WaitPlan.validate(None, "terminal", 55)
    assert plan.requested is None and plan.effective == wait_policy.UNTIL_TERMINAL_CEILING_SECONDS


async def test_all_three_tools_refuse_bad_requests(tmp_path):
    add_task("t", tmp_path)
    for call in (
        server.wait_for_task("t", 3601),
        server.wait_for_task("t", 5, until="terminal"),
        server.wait_for_tasks(["t"], "any", 3601),
        server.wait_for_tasks(["t"], "any", 5, "terminal"),
        server.wait_for_workflow("r", 3601),
        server.wait_for_workflow("r", 5, "terminal"),
    ):
        with pytest.raises(MCPError, match="timeout_seconds|until"):
            await call


async def test_wait_for_task_accepts_zero(tmp_path):
    add_task("t", tmp_path)
    result = await server.wait_for_task("t", 0)
    assert result["status"] == "running" and result["stop_reason"] == "timeout"
    assert result["timed_out"] and result["effective_timeout_seconds"] == 0
    assert "get_task_status" in result["next_step"]


# --- wait_for_task ----------------------------------------------------------------------------

async def test_wait_for_task_honours_more_than_45_seconds(tmp_path, fast_clock):
    add_task("t", tmp_path)
    result = await server.wait_for_task("t", 120)
    assert result["requested_timeout_seconds"] == result["effective_timeout_seconds"] == 120
    assert result["until"] == "timeout" and result["timed_out"]
    assert result["stop_reason"] == "timeout" and result["elapsed_seconds"] >= 120
    assert result["polling_guidance"] and "45" not in result["polling_guidance"]


@pytest.mark.parametrize("final", ["completed", "failed", "cancelled", "timed_out"])
async def test_wait_for_task_until_terminal_returns_on_every_terminal_status(tmp_path, final):
    task = add_task("t", tmp_path)
    waiter = asyncio.create_task(server.wait_for_task("t", until="terminal"))
    await asyncio.sleep(0.02)
    assert not waiter.done()
    task.status = final
    task.done.set()
    result = await waiter
    assert result["status"] == final and result["stop_reason"] == "terminal"
    assert not result["timed_out"] and result["until"] == "terminal"
    assert result["requested_timeout_seconds"] is None
    assert "next_step" not in result


async def test_wait_for_task_until_terminal_stops_at_the_ceiling(tmp_path, fast_clock, monkeypatch):
    monkeypatch.setattr(wait_policy, "UNTIL_TERMINAL_CEILING_SECONDS", 10)
    add_task("t", tmp_path)
    result = await server.wait_for_task("t", until="terminal")
    assert result["stop_reason"] == "ceiling" and result["timed_out"]
    assert result["effective_timeout_seconds"] == 10


async def test_recovered_dead_unobserved_record_is_not_reported_as_settled(tmp_path, fast_clock, monkeypatch):
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: False)
    write_record(tmp_path)
    expected = store.resolve_status(server._reg().log_dir, store.read(server._reg().log_dir, "rec"))[0]
    for kwargs in ({"until": "terminal"}, {"timeout_seconds": 5}):
        result = await server.wait_for_task("rec", **kwargs)
        assert result["stop_reason"] == "observation_unavailable" and not result["timed_out"]
        assert result["outcome_observed"] is False
        assert result["status"] == expected  # exactly what resolve_status reports, nothing invented
        assert "get_task_status" in result["polling_guidance"]


async def test_recovered_observed_terminal_record_still_settles(tmp_path, fast_clock, monkeypatch):
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: False)
    write_record(tmp_path, status="completed", exit_code=0)
    result = await server.wait_for_task("rec", until="terminal")
    assert result["stop_reason"] == "terminal" and result["outcome_observed"] is True


async def test_recovered_uncertain_identity_keeps_waiting(tmp_path, fast_clock, monkeypatch):
    """A real undecidable verdict (ps failed), not a stubbed liveness answer."""
    monkeypatch.setattr(wait_policy, "UNTIL_TERMINAL_CEILING_SECONDS", 10)
    checks = []
    def check_detail(candidate):
        checks.append(candidate)
        return "undecidable", "ps_failed"
    monkeypatch.setattr(identity, "check_detail", check_detail)
    write_record(tmp_path, start_time="Mon Jan  1 00:00:00 2024")
    result = await server.wait_for_task("rec", until="terminal")
    assert result["stop_reason"] == "ceiling" and result["status"] == "running"
    assert len(checks) > 3 and "outcome_observed" not in result  # still running: nothing observed


async def test_unavailable_wait_survives_a_snapshot_that_raises(tmp_path, fast_clock, monkeypatch):
    record = write_record(tmp_path)
    monkeypatch.setattr(store, "record_process_alive", lambda record: True)
    reads = []
    monkeypatch.setattr(server._reg(), "recover", lambda task_id: record if not reads and not reads.append(1) else None)
    def boom(*a, **k):
        raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")
    monkeypatch.setattr(store, "snapshot", boom)
    result = await server.wait_for_task("rec", until="terminal")
    assert result["stop_reason"] == "observation_unavailable" and result["task_id"] == "rec"
    assert result["status"] == "unknown" and "get_task_status" in result["next_step"]


async def test_snapshot_failure_on_a_normal_stop_still_raises(tmp_path, monkeypatch):
    write_record(tmp_path)
    monkeypatch.setattr(store, "record_process_alive", lambda record: True)
    monkeypatch.setattr(store, "snapshot", lambda *a, **k: (_ for _ in ()).throw(OSError("x")))
    with pytest.raises(OSError):
        await server.wait_for_task("rec", 0)


async def test_task_that_settled_as_the_hold_ran_out_is_not_reported_timed_out(tmp_path, monkeypatch):
    task = add_task("t", tmp_path)
    async def expire(task_, plan, ctx):
        task.status = "completed"
        task.done.set()
        return wait_policy.STOP_TIMEOUT
    monkeypatch.setattr(server, "_await_with_progress", expire)
    result = await server.wait_for_task("t", 1)
    assert result["status"] == "completed" and result["stop_reason"] == "terminal"
    assert result["timed_out"] is False


async def test_recovered_disappearing_record_reports_observation_unavailable(tmp_path, fast_clock, monkeypatch):
    record = write_record(tmp_path)
    monkeypatch.setattr(store, "record_process_alive", lambda record: True)
    reads = []
    def recover(task_id):
        reads.append(task_id)
        return record if len(reads) == 1 else None
    monkeypatch.setattr(server._reg(), "recover", recover)
    result = await server.wait_for_task("rec", until="terminal")
    assert result["stop_reason"] == "observation_unavailable" and not result["timed_out"]
    assert "get_task_status" in result["next_step"]


async def test_bounded_wait_on_vanished_record_keeps_its_timeout(tmp_path, fast_clock, monkeypatch):
    record = write_record(tmp_path)
    monkeypatch.setattr(store, "record_process_alive", lambda record: True)
    calls = iter([record])
    monkeypatch.setattr(server._reg(), "recover", lambda task_id: next(calls, None))
    result = await server.wait_for_task("rec", 5)
    assert result["stop_reason"] == "timeout"


# --- wait_for_tasks ---------------------------------------------------------------------------

def _patch_recovered(monkeypatch, rows):
    """rows: iterator of (status, source) per read; the last repeats."""
    monkeypatch.setattr(task_waiting, "_record_exists", lambda *a: True)
    source = SimpleNamespace(backend="codex", session_id=None, title=None)
    sequence = list(rows)
    state = {"i": 0}
    def read(*a):
        item = sequence[min(state["i"], len(sequence) - 1)]
        state["i"] += 1
        if item is None:
            return None
        status, status_source = item
        return source, status, None, status_source
    monkeypatch.setattr(task_waiting, "_recovered", read)
    monkeypatch.setattr(task_waiting, "POLL_SECONDS", 0.001)


async def test_wait_for_tasks_honours_more_than_45_seconds(monkeypatch, fake_clock):
    _patch_recovered(monkeypatch, [("running", "recovered_record")])
    registry = SimpleNamespace(get=lambda _: None, log_dir=None)
    result = await task_waiting.wait_for_tasks(registry, ["r"], timeout_seconds=120)
    assert result["effective_timeout_seconds"] == 120 and result["timed_out"]
    assert result["stop_reason"] == "timeout" and result["elapsed_seconds"] >= 120
    assert fake_clock[0] >= 220


@pytest.mark.parametrize("final", ["completed", "failed", "cancelled", "timed_out"])
async def test_wait_for_tasks_until_terminal_returns_on_terminal_status(tmp_path, final):
    task = add_task("t", tmp_path)
    waiter = asyncio.create_task(server.wait_for_tasks(["t"], "all", until="terminal"))
    await asyncio.sleep(0.02)
    assert not waiter.done()
    task.status = final
    task.done.set()
    result = await asyncio.wait_for(waiter, 5)
    assert result["stop_reason"] == "terminal" and result["terminal_task_ids"] == ["t"]
    assert result["undecidable_task_ids"] == []


async def test_wait_for_tasks_any_vs_all_with_until_terminal(tmp_path):
    add_task("a", tmp_path, "completed")
    add_task("b", tmp_path)
    assert (await server.wait_for_tasks(["a", "b"], "any", until="terminal"))["terminal_task_ids"] == ["a"]
    waiter = asyncio.create_task(server.wait_for_tasks(["a", "b"], "all", until="terminal"))
    await asyncio.sleep(0.02)
    assert not waiter.done()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter


async def test_wait_for_tasks_ceiling(monkeypatch, fake_clock):
    monkeypatch.setattr(wait_policy, "UNTIL_TERMINAL_CEILING_SECONDS", 30)
    _patch_recovered(monkeypatch, [("running", "recovered_record")])
    registry = SimpleNamespace(get=lambda _: None, log_dir=None)
    result = await task_waiting.wait_for_tasks(registry, ["r"], until="terminal")
    assert result["stop_reason"] == "ceiling" and result["timed_out"]
    assert result["elapsed_seconds"] >= 30 and result["effective_timeout_seconds"] == 30


async def test_dead_unobserved_counts_as_settled_in_both_modes(monkeypatch, fake_clock):
    _patch_recovered(monkeypatch, [("unknown", "recovered_dead")])
    for kwargs in ({"timeout_seconds": 10}, {"until": "terminal"}):
        result = await server.wait_for_tasks(["r"], "all", **kwargs)
        assert result["undecidable_task_ids"] == ["r"] and result["stop_reason"] == "terminal"
        assert result["terminal_task_ids"] == [] and not result["timed_out"]
        assert "undecidable_task_ids" in result["polling_guidance"]
        assert "nothing further" not in result["polling_guidance"]
    any_mode = await server.wait_for_tasks(["r"], "any", 10)
    assert any_mode["stop_reason"] == "terminal"


async def test_uncertain_identity_keeps_waiting(tmp_path, monkeypatch, fake_clock):
    """Real record, real header projection, an undecidable identity verdict."""
    monkeypatch.setattr(wait_policy, "UNTIL_TERMINAL_CEILING_SECONDS", 30)
    verdicts = []
    def verdict(candidate):
        verdicts.append(candidate)
        return "undecidable"
    monkeypatch.setattr(identity, "identity_check", verdict)
    write_record(tmp_path)
    result = await server.wait_for_tasks(["rec"], "any", until="terminal")
    assert result["stop_reason"] == "ceiling" and result["elapsed_seconds"] >= 30
    assert result["undecidable_task_ids"] == [] and result["tasks"][0]["status"] == "unknown"
    assert len(verdicts) > 5  # it kept observing rather than settling


async def test_dead_identity_is_undecidable_through_the_real_projection(tmp_path, monkeypatch, fake_clock):
    monkeypatch.setattr(identity, "identity_check", lambda candidate: "dead")
    write_record(tmp_path)
    result = await server.wait_for_tasks(["rec"], "any", until="terminal")
    assert result["undecidable_task_ids"] == ["rec"] and result["stop_reason"] == "terminal"


async def test_mode_any_guidance_admits_other_tasks_are_active(tmp_path):
    add_task("a", tmp_path, "completed")
    add_task("b", tmp_path)
    result = await server.wait_for_tasks(["a", "b"], "any", 0)
    assert result["stop_reason"] == "terminal"
    assert "still active" in result["polling_guidance"]
    done = await server.wait_for_tasks(["a"], "all", 0)
    assert "still active" not in done["polling_guidance"]


async def test_persistent_unavailability_stops_until_terminal_waits(monkeypatch, fake_clock):
    _patch_recovered(monkeypatch, [("running", "recovered_record"), None])
    result = await server.wait_for_tasks(["r"], "all", until="terminal")
    assert result["stop_reason"] == "observation_unavailable" and not result["timed_out"]
    assert result["tasks"][0]["status"] == "unavailable"


async def test_transient_unavailability_recovers(monkeypatch, fake_clock):
    _patch_recovered(monkeypatch, [("running", "recovered_record"), None, None,
                                   ("completed", "recovered_record")])
    result = await server.wait_for_tasks(["r"], "all", until="terminal")
    assert result["stop_reason"] == "terminal" and result["terminal_task_ids"] == ["r"]


async def test_bounded_wait_ignores_persistent_unavailability(monkeypatch, fake_clock):
    _patch_recovered(monkeypatch, [("running", "recovered_record"), None])
    result = await server.wait_for_tasks(["r"], "all", 10)
    assert result["stop_reason"] == "timeout" and result["timed_out"] is True
    assert result["tasks"][0]["status"] == "unavailable"
    assert result["elapsed_seconds"] >= 10


async def test_rows_carry_bounded_failure_diagnostic(tmp_path):
    task = add_task("t", tmp_path, "failed")
    task.acc.failure_diagnostic = {"category": "usage_limit", "reason": "r" * 5000, "source": "x"}
    plain = add_task("p", tmp_path, "completed")
    result = await server.wait_for_tasks(["t", "p"], "all", 0)
    diagnostic = result["tasks"][0]["failure_diagnostic"]
    assert set(diagnostic) == {"category", "reason"} and diagnostic["category"] == "usage_limit"
    assert len(diagnostic["reason"].encode()) <= task_waiting.FAILURE_REASON_BYTES
    assert "failure_diagnostic" not in result["tasks"][1] and plain.done.is_set()


# --- wait_for_workflow ------------------------------------------------------------------------

def workflow_status(monkeypatch, statuses, touch=True):
    """statuses: list of dicts, or Exception instances to raise; the last item repeats."""
    calls = []
    path = server._reg().log_dir.parent / "workflow-runs" / "r.json"
    async def call(action, **kwargs):
        item = statuses[min(len(calls), len(statuses) - 1)]
        calls.append(item)
        if touch:
            path.write_text("x" * len(calls))  # a real run's file changes when its status does
        if isinstance(item, Exception):
            raise item
        return {"workflow_run_id": kwargs["run_id"], **item}
    monkeypatch.setattr(server, "_workflow_call", call)
    return calls


async def test_workflow_honours_more_than_45_seconds(monkeypatch, fast_clock):
    workflow_status(monkeypatch, [{"status": "running"}])
    result = await server.wait_for_workflow("r", 120)
    assert result["effective_timeout_seconds"] == 120 and result["timed_out"]
    assert result["stop_reason"] == "timeout" and result["elapsed_seconds"] >= 120
    assert "45" not in result["polling_guidance"]


@pytest.mark.parametrize("final", ["completed", "failed", "cancelled"])
async def test_workflow_until_terminal_returns_on_terminal(monkeypatch, fast_clock, final):
    workflow_status(monkeypatch, [{"status": "running"}, {"status": "running"}, {"status": final}])
    result = await server.wait_for_workflow("r", until="terminal")
    assert result["status"] == final and result["stop_reason"] == "terminal"
    assert result["until"] == "terminal" and not result["timed_out"]


@pytest.mark.parametrize("state", ["needs_input", "needs_attention", "paused"])
async def test_workflow_attention_states_stop_the_wait(monkeypatch, fast_clock, state):
    workflow_status(monkeypatch, [{"status": "running"}, {"status": state}])
    result = await server.wait_for_workflow("r", until="terminal")
    assert result["status"] == state and result["stop_reason"] == "attention"
    assert not result["timed_out"]


async def test_workflow_terminal_but_settling_keeps_waiting(monkeypatch, fast_clock):
    calls = workflow_status(monkeypatch, [{"status": "completed", "settling": True},
                                          {"status": "completed", "settling": True},
                                          {"status": "completed", "settling": False}])
    result = await server.wait_for_workflow("r", 60)
    assert result["stop_reason"] == "terminal" and not result["settling"] and len(calls) == 3


async def test_workflow_settling_forever_times_out(monkeypatch, fast_clock):
    workflow_status(monkeypatch, [{"status": "failed", "settling": True}])
    result = await server.wait_for_workflow("r", 10)
    assert result["stop_reason"] == "timeout" and result["timed_out"]


async def test_workflow_mid_wait_read_failure_is_retried(monkeypatch, fast_clock):
    workflow_status(monkeypatch, [{"status": "running"}, RuntimeError("x"), RuntimeError("x"),
                                  {"status": "completed"}])
    result = await server.wait_for_workflow("r", until="terminal")
    assert result["stop_reason"] == "terminal" and result["status"] == "completed"


async def test_workflow_persistent_read_failure_returns_last_good_snapshot(monkeypatch, fast_clock):
    calls = workflow_status(monkeypatch, [{"status": "running", "name": "wf"}, RuntimeError("x")])
    result = await server.wait_for_workflow("r", until="terminal")
    assert result["stop_reason"] == "observation_unavailable" and not result["timed_out"]
    assert result["status"] == "running" and result["name"] == "wf" and len(calls) == 4


async def test_workflow_initial_read_failure_still_raises(monkeypatch):
    workflow_status(monkeypatch, [ValueError("unknown run")])
    with pytest.raises(ValueError, match="unknown run"):
        await server.wait_for_workflow("r", 5)


# --- progress ---------------------------------------------------------------------------------

async def test_progress_is_sent_by_all_three_waits(tmp_path, fast_clock, monkeypatch):
    monkeypatch.setattr(task_waiting, "POLL_SECONDS", 0.001)
    add_task("t", tmp_path)
    ctx = Ctx()
    await server.wait_for_task("t", 1, ctx=ctx)
    assert ctx.reports and ctx.reports[0][1] == wait_policy.WaitPlan.validate(1, 'timeout', 55).progress_total

    ctx = Ctx()
    await server.wait_for_tasks(["t"], "any", 1, ctx=ctx)
    assert ctx.reports

    workflow_status(monkeypatch, [{"status": "running"}])
    ctx = Ctx()
    await server.wait_for_workflow("r", 60, ctx=ctx)
    assert ctx.reports and ctx.reports[0][1] == wait_policy.WaitPlan.validate(60, "timeout", 30).progress_total and "workflow r" in ctx.reports[0][2]


async def test_until_terminal_progress_has_no_total(tmp_path, fast_clock):
    task = add_task("t", tmp_path)
    ctx = Ctx()
    waiter = asyncio.create_task(server.wait_for_task("t", until="terminal", ctx=ctx))
    await asyncio.sleep(0.05)
    task.status = "completed"
    task.done.set()
    await waiter
    assert ctx.reports and all(report[1] is None for report in ctx.reports)


async def test_failing_progress_callback_never_breaks_a_wait(tmp_path, fast_clock, monkeypatch):
    monkeypatch.setattr(task_waiting, "POLL_SECONDS", 0.001)
    add_task("t", tmp_path)
    ctx = Ctx(fail=True)
    assert (await server.wait_for_task("t", 1, ctx=ctx))["stop_reason"] == "timeout"
    assert (await server.wait_for_tasks(["t"], "any", 1, ctx=ctx))["stop_reason"] == "timeout"
    workflow_status(monkeypatch, [{"status": "running"}])
    assert (await server.wait_for_workflow("r", 60, ctx=ctx))["stop_reason"] == "timeout"
    assert ctx.reports


# --- non-interference -------------------------------------------------------------------------

async def test_cancelling_waits_leaves_tasks_untouched(tmp_path):
    task = add_task("t", tmp_path)
    waiters = [asyncio.create_task(server.wait_for_task("t", until="terminal")),
               asyncio.create_task(server.wait_for_tasks(["t"], "all", until="terminal"))]
    await asyncio.sleep(0.02)
    for waiter in waiters:
        waiter.cancel()
    for waiter in waiters:
        with pytest.raises(asyncio.CancelledError):
            await waiter
    assert task.status == "running" and not task.cancel_requested and not task.done.is_set()
    assert not task.done._waiters


async def test_waiting_on_a_recovered_record_never_writes_it(tmp_path, fast_clock, monkeypatch):
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: False)
    write_record(tmp_path)
    path = store.record_path(server._reg().log_dir, "rec")
    before = path.read_bytes()
    monkeypatch.setattr(store, "write", lambda *a, **k: pytest.fail("a wait wrote a record"))
    await server.wait_for_task("rec", until="terminal")
    await server.wait_for_tasks(["rec"], "all", 0)
    assert path.read_bytes() == before


async def test_cancelling_a_recovered_wait_leaves_the_record_alone(tmp_path, monkeypatch):
    monkeypatch.setattr(wait_policy, "PROGRESS_INTERVAL_SECONDS", 0.005)
    monkeypatch.setattr(store, "record_process_alive", lambda record: True)
    write_record(tmp_path)
    path = store.record_path(server._reg().log_dir, "rec")
    before = path.read_bytes()
    waiter = asyncio.create_task(server.wait_for_task("rec", until="terminal"))
    await asyncio.sleep(0.03)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert path.read_bytes() == before


# --- settling stall, ctl, strict ints ---------------------------------------------------------

async def test_workflow_settling_with_no_file_change_stops_an_until_terminal_wait(monkeypatch, fast_clock):
    monkeypatch.setattr(server, "WORKFLOW_SETTLING_STALL_SECONDS", 20.0)
    workflow_status(monkeypatch, [{"status": "completed", "settling": True}], touch=False)
    result = await server.wait_for_workflow("r", until="terminal")
    assert result["stop_reason"] == "observation_unavailable" and not result["timed_out"]
    assert result["settling"] and result["elapsed_seconds"] >= 20
    # A bounded wait is unaffected by the stall rule.
    bounded = await server.wait_for_workflow("r", 60)
    assert bounded["stop_reason"] == "timeout"


async def test_workflow_settling_that_makes_progress_is_not_stalled(monkeypatch, fast_clock, workflow_run_file):
    monkeypatch.setattr(server, "WORKFLOW_SETTLING_STALL_SECONDS", 20.0)
    run_file = workflow_run_file
    counter = {"n": 0}
    async def call(action, **kwargs):
        counter["n"] += 1
        run_file.write_text("x" * counter["n"])  # the run file keeps changing while settling
        return {"workflow_run_id": "r", "status": "completed", "settling": counter["n"] < 40}
    monkeypatch.setattr(server, "_workflow_call", call)
    result = await server.wait_for_workflow("r", until="terminal")
    assert result["stop_reason"] == "terminal"


def test_ctl_workflow_wait_passes_until_and_timeout(monkeypatch, capsys):
    seen = []
    async def wait(run_id, timeout_seconds, until):
        seen.append((run_id, timeout_seconds, until))
        return {"workflow_run_id": run_id, "status": "completed", "stop_reason": "terminal"}
    monkeypatch.setattr(server, "_wait_workflow_full", wait)
    assert ctl.main(["workflow-wait", "r1", "--until", "terminal", "--json"]) == 0
    assert ctl.main(["workflow-wait", "r1", "--timeout-seconds", "90", "--json"]) == 0
    assert seen == [("r1", None, "terminal"), ("r1", 90, "timeout")]


@pytest.mark.parametrize("value", [1.5, "5", True])
async def test_mcp_tools_reject_non_integer_timeouts(value):
    from mcp import Client
    async with Client(server.mcp) as client:
        for name, args in (("wait_for_task", {"task_id": "t"}),
                           ("wait_for_tasks", {"task_ids": ["t"]}),
                           ("wait_for_workflow", {"workflow_run_id": "r"})):
            response = await client.call_tool(name, {**args, "timeout_seconds": value})
            assert response.is_error, (name, value)


# --- simulated transport interruption ---------------------------------------------------------

async def test_interrupting_every_wait_mid_hold_leaves_everything_untouched(tmp_path, monkeypatch):
    import subprocess
    monkeypatch.setattr(wait_policy, "PROGRESS_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(server, "WORKFLOW_POLL_SECONDS", 0.01)
    monkeypatch.setattr(task_waiting, "POLL_SECONDS", 0.01)
    process = subprocess.Popen(["sleep", "60"])
    try:
        live = add_task("live", tmp_path)
        record = write_record(tmp_path, pid=process.pid, status="running")
        path = store.record_path(server._reg().log_dir, "rec")
        before = path.read_bytes()
        monkeypatch.setattr(store, "write", lambda *a, **k: pytest.fail("a wait wrote a record"))
        monkeypatch.setattr(store, "record_process_alive", lambda record: True)
        workflow_status(monkeypatch, [{"status": "running"}])
        baseline = len(asyncio.all_tasks())
        waiters = [
            asyncio.create_task(server.wait_for_task("live", until="terminal")),
            asyncio.create_task(server.wait_for_task("rec", until="terminal")),
            asyncio.create_task(server.wait_for_tasks(["live", "rec"], "all", until="terminal")),
            asyncio.create_task(server.wait_for_workflow("r", until="terminal")),
        ]
        await asyncio.sleep(0.1)
        assert all(not waiter.done() for waiter in waiters)
        for waiter in waiters:  # the transport went away
            waiter.cancel()
        for waiter in waiters:
            with pytest.raises(asyncio.CancelledError):
                await waiter
        await asyncio.sleep(0)
        assert live.status == "running" and not live.cancel_requested and not live.done.is_set()
        assert not live.done._waiters
        assert path.read_bytes() == before and record.status == "running"
        assert process.poll() is None
        assert len(asyncio.all_tasks()) == baseline
    finally:
        process.kill()
        process.wait()


async def test_unreadable_run_file_is_an_unavailable_read_not_an_unchanged_one(monkeypatch, fast_clock, workflow_run_file):
    monkeypatch.setattr(server, "WORKFLOW_SETTLING_STALL_SECONDS", 100000.0)
    workflow_status(monkeypatch, [{"status": "completed", "settling": True}], touch=False)
    workflow_run_file.unlink()
    result = await server.wait_for_workflow("r", until="terminal")
    assert result["stop_reason"] == "observation_unavailable"
    assert result["elapsed_seconds"] < 100000  # stopped by the 3-round rule, not the stall clock


async def test_workflow_run_file_comes_from_the_registry_root_not_home(monkeypatch, fast_clock, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path / "elsewhere"))
    monkeypatch.setattr(server.Path, "home", classmethod(lambda cls: tmp_path / "elsewhere"))
    workflow_status(monkeypatch, [{"status": "running"}, {"status": "completed"}])
    result = await server.wait_for_workflow("r", until="terminal")
    assert result["stop_reason"] == "terminal"


# --- recovered task whose owning server is still alive ----------------------------------------

def _owner_stub(monkeypatch, owner_state):
    """The task process (pid 4321) is dead; the owner (pid 999) reports `owner_state`."""
    def check(candidate):
        return "dead" if candidate and candidate.get("pid") == 4321 else owner_state[0]
    monkeypatch.setattr(identity, "identity_check", check)
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: False)


async def test_wait_for_task_keeps_observing_while_the_owner_is_draining(tmp_path, fast_clock, monkeypatch):
    monkeypatch.setattr(wait_policy, "OWNER_DRAIN_GRACE_SECONDS", 1000.0)
    monkeypatch.setattr(wait_policy, "UNTIL_TERMINAL_CEILING_SECONDS", 30)
    owner = ["alive"]
    _owner_stub(monkeypatch, owner)
    write_record(tmp_path, owner={"pid": 999, "start_time": "x"})
    result = await server.wait_for_task("rec", until="terminal")
    # It kept observing for the whole hold (the owner is still persisting the outcome), and only
    # the final reconciliation reports the still-unobserved outcome.
    assert result["elapsed_seconds"] >= 30
    assert result["stop_reason"] == "observation_unavailable" and result["outcome_observed"] is False


async def test_wait_for_task_stops_once_the_owner_is_gone_or_the_grace_lapses(tmp_path, fast_clock, monkeypatch):
    owner = ["dead"]
    _owner_stub(monkeypatch, owner)
    write_record(tmp_path, owner={"pid": 999, "start_time": "x"})
    result = await server.wait_for_task("rec", until="terminal")
    assert result["stop_reason"] == "observation_unavailable" and result["outcome_observed"] is False
    owner[0] = "alive"
    monkeypatch.setattr(wait_policy, "OWNER_DRAIN_GRACE_SECONDS", 20.0)
    result = await server.wait_for_task("rec", until="terminal")
    assert result["stop_reason"] == "observation_unavailable" and result["elapsed_seconds"] >= 20


async def test_final_snapshot_reconciliation_applies_the_unobserved_rule(tmp_path, monkeypatch):
    """Alive while polling, dead by the final snapshot: the hold's expiry must not read as terminal."""
    record = write_record(tmp_path)
    alive = [True]
    monkeypatch.setattr(store, "record_process_alive", lambda r: alive[0])
    async def poll(rec, plan, ctx):
        alive[0] = False  # the process dies after the last poll, before the final snapshot
        return rec, wait_policy.STOP_TIMEOUT, True
    monkeypatch.setattr(server, "_poll_recovered", poll)
    result = await server.wait_for_task("rec", 1)
    assert result["stop_reason"] == "observation_unavailable" and result["timed_out"] is False
    assert result["outcome_observed"] is False and record.status == "failed"


async def test_bounded_wait_keeps_timeout_when_the_owner_is_still_alive(tmp_path, monkeypatch):
    """The hold ran out with the owner alive and persisting: `timeout`, not observation_unavailable."""
    record = write_record(tmp_path, owner={"pid": 999, "start_time": "x"})
    _owner_stub(monkeypatch, ["alive"])
    monkeypatch.setattr(store, "record_process_alive", lambda r: True)
    async def poll(rec, plan, ctx):
        monkeypatch.setattr(store, "record_process_alive", lambda r: False)
        return rec, wait_policy.STOP_TIMEOUT, True
    monkeypatch.setattr(server, "_poll_recovered", poll)
    result = await server.wait_for_task("rec", 1)
    assert result["stop_reason"] == "timeout" and result["timed_out"] is True
    assert result["outcome_observed"] is False and record.status == "failed"
    _owner_stub(monkeypatch, ["dead"])
    result = await server.wait_for_task("rec", 1)
    assert result["stop_reason"] == "observation_unavailable"


async def test_outcome_observed_is_absent_on_a_running_recovered_task(tmp_path, fast_clock, monkeypatch):
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: True)
    write_record(tmp_path)
    result = await server.wait_for_task("rec", 5)
    assert result["stop_reason"] == "timeout" and result["status"] == "running"
    assert "outcome_observed" not in result


async def test_wait_for_tasks_keeps_observing_while_the_owner_is_draining(tmp_path, monkeypatch, fake_clock):
    owner = ["alive"]
    _owner_stub(monkeypatch, owner)
    write_record(tmp_path, owner={"pid": 999, "start_time": "x"})
    monkeypatch.setattr(wait_policy, "OWNER_DRAIN_GRACE_SECONDS", 20.0)
    result = await server.wait_for_tasks(["rec"], "all", 10)
    assert result["stop_reason"] == "timeout" and result["undecidable_task_ids"] == []
    assert result["tasks"][0]["status_source"] == "recovered_owner_draining"
    result = await server.wait_for_tasks(["rec"], "all", until="terminal")
    assert result["stop_reason"] == "terminal" and result["undecidable_task_ids"] == ["rec"]
    assert result["elapsed_seconds"] >= 20
    owner[0] = "dead"
    result = await server.wait_for_tasks(["rec"], "all", 10)
    assert result["undecidable_task_ids"] == ["rec"] and result["elapsed_seconds"] < 2


async def test_live_task_reports_outcome_observed_once_finished(tmp_path):
    add_task("t", tmp_path)
    running = await server.wait_for_task("t", 0)
    assert "outcome_observed" not in running
    server._reg().get("t").status = "completed"
    server._reg().get("t").done.set()
    assert (await server.wait_for_task("t", 0))["outcome_observed"] is True


@pytest.mark.parametrize("item,reason", [
    ({"status": "completed", "settling": False}, "terminal"),
    ({"status": "failed", "settling": False}, "terminal"),
    ({"status": "needs_attention"}, "attention"),
    ({"status": "some_new_status"}, "attention"),
])
async def test_workflow_wait_follows_shared_phase(monkeypatch, fast_clock, item, reason):
    from polybridge import wait_policy
    workflow_status(monkeypatch, [{"status": "running"}, item])
    result = await server.wait_for_workflow("r", until="terminal")
    assert result["stop_reason"] == reason
    assert wait_policy.workflow_phase(item["status"], bool(item.get("settling"))) == reason


async def test_workflow_wait_settling_phase_holds(monkeypatch, fast_clock):
    from polybridge import wait_policy
    assert wait_policy.workflow_phase("cancelled", True) == "settling"
    workflow_status(monkeypatch, [{"status": "cancelled", "settling": True}])
    result = await server.wait_for_workflow("r", 10)
    assert result["stop_reason"] == "timeout"
