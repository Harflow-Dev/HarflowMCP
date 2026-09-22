"""Registry bookkeeping, using tasks with no real subprocess behind them."""

from __future__ import annotations

import asyncio
import signal
import subprocess
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from polybridge import backends
from polybridge.backends.base import FREEDOMS
from polybridge.backends.base import Enforcement
from polybridge.backends.codex import CodexBackend
from polybridge import store
from polybridge import tasks as tasks_module
from polybridge.tasks import (
    RepoUnavailableError,
    SessionBusyError,
    Task,
    TaskRegistry,
)


def make_task(
    tmp_path: Path,
    task_id: str,
    *,
    session_id: str = "session",
    finished: bool = False,
    age_seconds: int = 0,
) -> Task:
    task = Task(
        task_id=task_id,
        backend="claude",
        session_id=session_id,
        repo_path=tmp_path,
        prompt="x",
        max_turns=5,
        log_path=tmp_path / f"{task_id}.jsonl",
        started_at=datetime.now(timezone.utc) - timedelta(seconds=age_seconds),
    )
    if finished:
        task.status = "completed"
        task.done.set()
    return task


async def test_prune_trims_finished_tasks_oldest_first(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path, max_tasks=2)
    for index in range(4):
        task = make_task(tmp_path, f"t{index}", finished=True, age_seconds=100 - index)
        registry._tasks[task.task_id] = task

    registry.prune()

    assert [t.task_id for t in registry.list()] == ["t2", "t3"]


async def test_prune_never_evicts_a_live_task(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path, max_tasks=1)
    live = make_task(tmp_path, "live", age_seconds=100)
    finished = make_task(tmp_path, "finished", finished=True, age_seconds=50)
    registry._tasks[live.task_id] = live
    registry._tasks[finished.task_id] = finished

    registry.prune()

    # The live task is older, but capacity is reclaimed from finished tasks only.
    assert [t.task_id for t in registry.list()] == ["live"]


async def test_registry_may_exceed_capacity_while_everything_is_live(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path, max_tasks=1)
    for index in range(3):
        task = make_task(tmp_path, f"t{index}")
        registry._tasks[task.task_id] = task

    registry.prune()

    assert len(registry.list()) == 3


async def test_a_finishing_task_makes_room(tmp_path: Path) -> None:
    """Eviction must not depend on a new task being started."""
    registry = TaskRegistry(log_dir=tmp_path, max_tasks=1)
    for index in range(3):
        task = make_task(tmp_path, f"t{index}", age_seconds=100 - index)
        registry._tasks[task.task_id] = task

    for task in registry.list():
        task.status = "completed"
        task.done.set()
    registry.prune()

    assert [t.task_id for t in registry.list()] == ["t2"]


def test_session_is_busy_only_while_a_run_is_live(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path)
    task = make_task(tmp_path, "t0", session_id="s1")
    registry._tasks[task.task_id] = task

    assert registry.session_has_live_run("s1")
    assert not registry.session_has_live_run("s2")

    task.done.set()
    assert not registry.session_has_live_run("s1")


async def test_resume_refuses_while_the_session_has_a_live_run(tmp_path: Path) -> None:
    """Two processes on one --resume session would fight over the same conversation state."""
    registry = TaskRegistry(log_dir=tmp_path)
    parent = make_task(tmp_path, "parent", session_id="s1", finished=True)
    sibling = make_task(tmp_path, "sibling", session_id="s1")
    registry._tasks[parent.task_id] = parent
    registry._tasks[sibling.task_id] = sibling

    with pytest.raises(SessionBusyError):
        await registry.resume(parent, "carry on")


async def test_resume_runs_at_the_parents_reasoning_effort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Carried over for the same reason `model` is: the continuation must run — and be reported —
    at the effort the session started with, not silently drop back to the CLI's own default.

    Asserts on the *argv itself*, not just the kwargs the patched _spawn was called with: a
    resume that built the argv without the effort flag but still reported the metadata kwarg would
    have passed the weaker check this replaces.
    """
    registry = TaskRegistry(log_dir=tmp_path)
    parent = make_task(tmp_path, "parent", session_id="s1", finished=True)
    parent.reasoning_effort = "high"
    registry._tasks[parent.task_id] = parent

    captured: dict = {}

    async def fake_spawn(argv, **kwargs):
        captured["argv"] = argv
        captured.update(kwargs)
        child = make_task(tmp_path, "child", session_id="s1")
        child.reasoning_effort = kwargs.get("reasoning_effort")
        return child

    monkeypatch.setattr(registry, "_spawn", fake_spawn)

    child = await registry.resume(parent, "carry on")

    assert captured["reasoning_effort"] == "high"
    assert child.reasoning_effort == "high"
    # claude's own effort flag, carrying the actual requested value — not just the metadata kwarg.
    argv = captured["argv"]
    assert argv[argv.index("--effort") + 1] == "high"


async def test_resume_record_runs_at_the_recorded_reasoning_effort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same carry-over, via the path recovering a task this process never spawned. Also asserts on
    the argv, not just the kwargs — see test_resume_runs_at_the_parents_reasoning_effort."""
    registry = TaskRegistry(log_dir=tmp_path)
    record = store.TaskRecord(
        task_id="old",
        backend="claude",
        session_id="s1",
        markers=["s1"],
        repo_path=str(tmp_path),
        started_at=datetime.now(timezone.utc).isoformat(),
        status="completed",
        reasoning_effort="xhigh",
    )

    captured: dict = {}

    async def fake_spawn(argv, **kwargs):
        captured["argv"] = argv
        captured.update(kwargs)
        child = make_task(tmp_path, "child", session_id="s1")
        child.reasoning_effort = kwargs.get("reasoning_effort")
        return child

    monkeypatch.setattr(registry, "_spawn", fake_spawn)

    child = await registry.resume_record(record, "carry on")

    assert captured["reasoning_effort"] == "xhigh"
    assert child.reasoning_effort == "xhigh"
    argv = captured["argv"]
    assert argv[argv.index("--effort") + 1] == "xhigh"


async def test_resume_record_with_no_stored_effort_can_still_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A record persisted before this field existed deserializes reasoning_effort to None (the
    dataclass default) rather than failing to load — and that record must still be resumable, with
    no effort flag reaching the child's argv at all."""
    registry = TaskRegistry(log_dir=tmp_path)
    # No reasoning_effort kwarg at all: stands in for a pre-change on-disk record.
    record = store.TaskRecord(
        task_id="old",
        backend="claude",
        session_id="s1",
        markers=["s1"],
        repo_path=str(tmp_path),
        started_at=datetime.now(timezone.utc).isoformat(),
        status="completed",
    )
    assert record.reasoning_effort is None

    captured: dict = {}

    async def fake_spawn(argv, **kwargs):
        captured["argv"] = argv
        captured.update(kwargs)
        return make_task(tmp_path, "child", session_id="s1")

    monkeypatch.setattr(registry, "_spawn", fake_spawn)

    await registry.resume_record(record, "carry on")

    assert captured["reasoning_effort"] is None
    assert "--effort" not in captured["argv"]


async def test_finish_draining_abandons_pipes_held_open_after_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A grandchild holding stdout open must not make a task unfinishable."""
    monkeypatch.setattr(tasks_module, "DRAIN_GRACE_SECONDS", 0.05)
    task = make_task(tmp_path, "t0")
    never_eof = asyncio.create_task(asyncio.sleep(3600))
    task.watchers = [never_eof]

    await tasks_module._finish_draining(task)

    assert never_eof.cancelled()


async def test_finish_draining_returns_as_soon_as_the_drainers_do(tmp_path: Path) -> None:
    task = make_task(tmp_path, "t0")
    task.watchers = [asyncio.create_task(asyncio.sleep(0))]

    await asyncio.wait_for(tasks_module._finish_draining(task), timeout=1)

    assert all(watcher.done() and not watcher.cancelled() for watcher in task.watchers)


async def test_a_cancelled_monitor_leaves_a_live_run_recorded_as_running(tmp_path: Path) -> None:
    """A torn-down server must not record an outcome for a process that is still alive.

    This is what a client restarting the bridge does to every in-flight task. Writing a terminal
    status here was observed to strand live agent runs: `store.write` then refuses to correct the
    record, so no later server can see the process is still going. Uses a plain `sleep` process
    rather than a real agent run.
    """
    registry = TaskRegistry(log_dir=tmp_path)
    proc = await asyncio.create_subprocess_exec(
        "sleep",
        "30",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    task = make_task(tmp_path, "t0")
    task.proc = proc
    task.pgid = proc.pid
    registry._tasks[task.task_id] = task
    registry.persist(task)
    task.watchers = [
        asyncio.create_task(tasks_module._drain_stdout(task, registry)),
        asyncio.create_task(tasks_module._drain_stderr(task)),
    ]

    monitor = asyncio.create_task(tasks_module._monitor(task, registry))
    await asyncio.sleep(0.05)
    monitor.cancel()
    await asyncio.gather(monitor, return_exceptions=True)

    assert task.status == "running"
    assert not task.finished
    assert store.read(tmp_path, task.task_id).status == "running"

    tasks_module._signal_group(task, signal.SIGKILL)
    for watcher in task.watchers:
        watcher.cancel()
    await asyncio.gather(*task.watchers, return_exceptions=True)
    await proc.wait()


async def test_a_teardown_mid_cancellation_still_records_the_cancellation(tmp_path: Path) -> None:
    """Cancellation intent is the one thing recovery cannot reconstruct.

    A signalled run dies without a result event, so leaving the record `running` would have the next
    server infer `failed` and lose the fact that someone asked for this. Safe to record because a
    `cancelled` record carries no exit code, so it is rechecked against the process.
    """
    registry = TaskRegistry(log_dir=tmp_path)
    proc = await asyncio.create_subprocess_exec(
        "sleep",
        "30",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    task = make_task(tmp_path, "t0")
    task.proc = proc
    task.pgid = proc.pid
    task.cancel_requested = True
    registry._tasks[task.task_id] = task
    registry.persist(task)

    monitor = asyncio.create_task(tasks_module._monitor(task, registry))
    await asyncio.sleep(0.05)
    monitor.cancel()
    await asyncio.gather(monitor, return_exceptions=True)

    assert task.status == "cancelled"
    assert store.read(tmp_path, task.task_id).status == "cancelled"

    tasks_module._signal_group(task, signal.SIGKILL)
    await proc.wait()


async def test_a_monitor_that_crashes_still_publishes_a_terminal_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`done` must never be observable next to a non-terminal status.

    The cancellation path deliberately leaves the task running, so the `finally` backstop is still
    load-bearing for every other way the monitor can fail.
    """
    registry = TaskRegistry(log_dir=tmp_path)
    proc = await asyncio.create_subprocess_exec("true", start_new_session=True)
    task = make_task(tmp_path, "t0")
    task.proc = proc
    task.pgid = proc.pid
    registry._tasks[task.task_id] = task

    async def boom(_: Task) -> None:
        raise RuntimeError("drain bookkeeping exploded")

    monkeypatch.setattr(tasks_module, "_finish_draining", boom)

    await tasks_module._monitor(task, registry)

    assert task.finished
    assert task.status == "failed"
    assert store.read(tmp_path, task.task_id).status == "failed"


async def test_cancel_recovered_does_not_claim_success_it_cannot_deliver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Marking it cancelled when the signal failed would have later calls contradict this one."""
    registry = TaskRegistry(log_dir=tmp_path)
    record = store.TaskRecord(
        task_id="orphan",
        backend="claude",
        session_id="s1",
        markers=["s1"],
        repo_path=str(tmp_path),
        started_at=datetime.now(timezone.utc).isoformat(),
        pid=1234,
        pgid=None,  # nothing to signal
    )
    store.write(tmp_path, record)
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: True)

    result = await registry.cancel_recovered(record)

    assert result.status == "running"
    assert store.read(tmp_path, "orphan").status == "running"


async def test_cancel_recovered_waits_for_the_sigkill_to_land(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Returning while the group is still dying makes the response contradict itself.

    `store.resolve_status` rechecks liveness for a `cancelled` record, so answering a cancellation
    before the process is gone reports the task as still running.
    """
    monkeypatch.setattr(tasks_module, "SIGKILL_GRACE_SECONDS", 0.05)
    registry = TaskRegistry(log_dir=tmp_path)
    record = store.TaskRecord(
        task_id="orphan",
        backend="claude",
        session_id="s1",
        markers=["s1"],
        repo_path=str(tmp_path),
        started_at=datetime.now(timezone.utc).isoformat(),
        pid=1234,
        pgid=1234,
    )
    store.write(tmp_path, record)

    killed = False

    def signal_group(_: store.TaskRecord, sig: int) -> bool:
        nonlocal killed
        if sig == signal.SIGKILL:
            killed = True
        return True

    monkeypatch.setattr(tasks_module, "_signal_recorded_group", signal_group)
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: not killed)

    result = await registry.cancel_recovered(record)

    assert killed
    assert result.status == "cancelled"
    assert store.resolve_status(tmp_path, result)[0] == "cancelled"


async def test_cancel_recovered_of_an_already_dead_task_changes_nothing(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path)
    record = store.TaskRecord(
        task_id="orphan",
        backend="claude",
        session_id="s1",
        markers=["s1"],
        repo_path=str(tmp_path),
        started_at=datetime.now(timezone.utc).isoformat(),
        pid=999_999_999,
        pgid=999_999_999,
    )

    assert (await registry.cancel_recovered(record)).status == "running"


async def test_session_exclusivity_spans_server_processes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Another process's live run on this session must also block a resume."""
    registry = TaskRegistry(log_dir=tmp_path)
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: True)
    store.write(
        tmp_path,
        store.TaskRecord(
            task_id="elsewhere",
            backend="claude",
            session_id="shared",
            markers=["shared"],
            repo_path=str(tmp_path),
            started_at=datetime.now(timezone.utc).isoformat(),
            pid=4321,
        ),
    )

    assert registry.session_has_live_run("shared")
    assert not registry.session_has_live_run("unrelated")


async def test_resuming_a_task_whose_repo_is_gone_fails_clearly(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path)
    record = store.TaskRecord(
        task_id="old",
        backend="claude",
        session_id="s1",
        markers=["s1"],
        repo_path=str(tmp_path / "deleted-repo"),
        started_at=datetime.now(timezone.utc).isoformat(),
        status="completed",
    )

    with pytest.raises(RepoUnavailableError, match="no longer exists"):
        await registry.resume_record(record, "carry on")


async def test_cancelling_a_finished_task_is_a_no_op(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path)
    task = make_task(tmp_path, "t0", finished=True)
    registry._tasks[task.task_id] = task

    await registry.cancel(task)

    assert task.status == "completed"
    assert not task.cancel_requested


# --- the default-branch notice on a publish-authorized dispatch --------------------------------
# Real git repos rather than mocks: the whole point of this check is what git actually records, and
# the failure it guards against (guessing from a branch *name*) is invisible to a mock.


def _repo_with_remote(tmp_path: Path, *, branch: str, remotes: tuple[str, ...] = ("origin",)) -> Path:
    """A repo on `branch`, with each named remote pointing at a bare clone and origin/HEAD recorded."""
    bare = tmp_path / "bare.git"
    subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(bare)], check=True)

    repo = tmp_path / "work"
    repo.mkdir()
    run = lambda *a: subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True)
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    run("config", "user.email", "t@t.t")
    run("config", "user.name", "T")
    (repo / "f.txt").write_text("seed\n")
    run("add", "-A")
    run("commit", "-qm", "init")
    for remote in remotes:
        run("remote", "add", remote, str(bare))
        run("push", "-q", remote, "main")
        # Record the remote's HEAD locally; the detection never performs a network lookup.
        (repo / ".git" / "refs" / "remotes" / remote).mkdir(parents=True, exist_ok=True)
        (repo / ".git" / "refs" / "remotes" / remote / "HEAD").write_text(
            f"ref: refs/remotes/{remote}/main\n"
        )
    if branch != "main":
        run("checkout", "-q", "-b", branch)
    return repo


def _codex_enforcement(freedom: str, network: bool | None = None) -> Enforcement:
    """The real Enforcement a codex dispatch would carry, so these tests exercise the gate the
    way `_spawn` does rather than a hand-built block that could drift from it."""
    return CodexBackend().enforcement(freedom, network=network)


def test_publish_on_the_default_branch_discloses_it(tmp_path: Path) -> None:
    repo = _repo_with_remote(tmp_path, branch="main")

    notice = tasks_module._publish_branch_notice("publish", repo, _codex_enforcement("publish"))

    assert notice is not None
    assert "'main'" in notice
    # Disclosure, not a guard: start_task returns after the process has already spawned.
    assert "cancel this task" in notice.lower()
    # The branch is sampled just before the launch, not during the run — the notice must not
    # overstate how current it is.
    assert "sampled once" in notice
    assert "will not be re-checked" in notice


def test_publish_on_a_feature_branch_says_nothing(tmp_path: Path) -> None:
    repo = _repo_with_remote(tmp_path, branch="feature/x")

    assert tasks_module._publish_branch_notice("publish", repo, _codex_enforcement("publish")) is None


def test_a_feature_branch_named_main_is_not_mistaken_for_the_default(tmp_path: Path) -> None:
    """The reason the branch *name* is never used as evidence: a repo's default can be anything."""
    bare = tmp_path / "bare.git"
    subprocess.run(["git", "init", "-q", "--bare", "-b", "trunk", str(bare)], check=True)
    repo = tmp_path / "work"
    repo.mkdir()
    run = lambda *a: subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True)
    subprocess.run(["git", "init", "-q", "-b", "trunk", str(repo)], check=True)
    run("config", "user.email", "t@t.t")
    run("config", "user.name", "T")
    (repo / "f.txt").write_text("seed\n")
    run("add", "-A")
    run("commit", "-qm", "init")
    run("remote", "add", "origin", str(bare))
    run("push", "-q", "origin", "trunk")
    (repo / ".git" / "refs" / "remotes" / "origin").mkdir(parents=True, exist_ok=True)
    (repo / ".git" / "refs" / "remotes" / "origin" / "HEAD").write_text(
        "ref: refs/remotes/origin/trunk\n"
    )
    run("checkout", "-q", "-b", "main")

    # On a branch called `main`, but the repo's real default is `trunk` — so nothing to disclose.
    assert tasks_module._publish_branch_notice("publish", repo, _codex_enforcement("publish")) is None


@pytest.mark.parametrize("freedom", ["read_only", "write_in_repo"])
def test_below_publish_with_no_network_nothing_is_checked_at_all(
    tmp_path: Path, freedom: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Narrowed from "below publish" to "below publish *and* network blocked".

    Being below `publish` is no longer sufficient on its own: `network=True` at `write_in_repo`
    produces the same argv as `publish`, so the gate is the Enforcement block, not the ladder.
    """
    repo = _repo_with_remote(tmp_path, branch="main")

    def explode(*args, **kwargs):
        raise AssertionError("no git command may run for an unexposed run")

    monkeypatch.setattr(tasks_module.subprocess, "run", explode)

    assert tasks_module._publish_branch_notice(freedom, repo, _codex_enforcement(freedom)) is None


def test_write_in_repo_with_network_is_disclosed_though_it_never_authorized_publishing(
    tmp_path: Path,
) -> None:
    """The cell the old `freedom in PUBLISH_FREEDOMS` gate missed entirely.

    Mechanically identical to `publish`, so just as able to reach a remote — and codex has no
    per-command deny list to stop a push. Silence here was the bug.
    """
    repo = _repo_with_remote(tmp_path, branch="main")

    notice = tasks_module._publish_branch_notice(
        "write_in_repo", repo, _codex_enforcement("write_in_repo", network=True)
    )

    assert notice is not None
    assert "'main'" in notice
    assert "cancel this task" in notice.lower()
    # The positive claims, not merely "some notice appeared": this cell must say publishing was
    # NOT authorized while a remote is nonetheless reachable. Asserting only the branch name would
    # pass even if this cell wrongly reused the publish-authorized wording.
    assert "does not authorize" in notice
    assert "permits remote publication" in notice
    assert "'enabled'" in notice
    assert "is authorized to commit, push, and open a PR" not in notice


def test_publish_with_network_blocked_is_still_disclosed_but_worded_differently(
    tmp_path: Path,
) -> None:
    """The other crossed cell: publishing is authorized, but network-backed push is not possible.

    The notice must not claim "nothing here blocked that", and must not claim publishing is
    stopped outright either — a push to a local-path remote inside a writable root was measured
    to still succeed with `network_access=false`.
    """
    repo = _repo_with_remote(tmp_path, branch="main")

    notice = tasks_module._publish_branch_notice(
        "publish", repo, _codex_enforcement("publish", network=False)
    )

    assert notice is not None
    assert "'main'" in notice
    # Excluding one phrase is too weak on its own — it would accept the *unauthorized* wording,
    # which is false here. Assert all three truths this cell has to carry.
    assert "is authorized to commit, push, and open a PR" in notice
    # Whole clauses, not the nouns: "permits a network-backed push but blocks a local path" would
    # satisfy a bare `"network-backed push" in notice` while saying the exact opposite.
    assert "blocks a network-backed push" in notice
    assert "still succeeds" in notice
    assert "does not authorize" not in notice


def test_a_repo_with_no_remote_reports_that_it_could_not_tell(git_repo: Path) -> None:
    """Silence would be wrong here: for a publish-authorized run, not knowing is itself material."""
    subprocess.run(["git", "-C", str(git_repo), "commit", "-qm", "x", "--allow-empty"], check=True)

    notice = tasks_module._publish_branch_notice("publish", git_repo, _codex_enforcement("publish"))

    assert notice is not None
    assert "could not be determined" in notice


def test_several_remotes_with_no_origin_are_not_picked_between(tmp_path: Path) -> None:
    repo = _repo_with_remote(tmp_path, branch="main", remotes=("upstream", "fork"))

    notice = tasks_module._publish_branch_notice("publish", repo, _codex_enforcement("publish"))

    assert notice is not None
    assert "could not be determined" in notice


def test_a_detached_head_reports_that_it_could_not_tell(tmp_path: Path) -> None:
    repo = _repo_with_remote(tmp_path, branch="main")
    head = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    subprocess.run(["git", "-C", str(repo), "checkout", "-q", head], check=True)

    notice = tasks_module._publish_branch_notice("publish", repo, _codex_enforcement("publish"))

    assert notice is not None
    assert "could not be determined" in notice


class _TrivialBackend:
    """A backend that spawns a real but instant process, so `_spawn` can be exercised end to end.

    The doubles in tests/test_backends.py exist for the same reason: no real backend has the shape
    a given test needs. This one only has to get through `_spawn`.
    """

    name = "trivial"
    binary = "/bin/echo"
    capabilities = SimpleNamespace(chooses_session_id=False)

    def build_start_argv(self, prompt, **kwargs):
        return [self.binary, "{}"]

    def build_resume_argv(self, prompt, **kwargs):
        return [self.binary, "{}"]

    def assert_safe(self, argv, freedom, network=None):
        return None

    def enforcement(self, freedom, network=None):
        return Enforcement(freedom=freedom, mechanism="none", os_enforced=False,
                           writes_confined=False, caveats=("test double",))

    def ingest(self, event, acc):
        return None

    def classify(self, acc, exit_code):
        return "completed"


class _NetworkAwareBackend(_TrivialBackend):
    """A double whose Enforcement actually varies with the network request, standing in for codex.

    `_NoBarrierBackend` cannot catch a `_spawn` that computes enforcement from the wrong pair,
    because its answer is `not_controlled` either way — a mutation of
    `backend.enforcement(freedom, network)` to `(freedom, None)` was measured to leave the whole
    suite green. This double makes that disagreement observable, which is the point of `_spawn`
    computing it once for the notice, the Task and the record.
    """

    name = "netaware"
    capabilities = SimpleNamespace(
        chooses_session_id=False,
        network_control=SimpleNamespace(
            can_enable=("write_in_repo",), can_block=("write_in_repo",)
        ),
    )

    def enforcement(self, freedom, network=None):
        return Enforcement(
            freedom=freedom,
            mechanism="double",
            os_enforced=True,
            writes_confined=True,
            network_access="enabled" if network else "blocked",
            caveats=("test double",),
        )


class _NoBarrierBackend(_TrivialBackend):
    """Stands in for claude/opencode/vibe: accepts `network=True` because it imposes no barrier,
    and reports `not_controlled` — the exact combination the uncontrolled-network notice exists
    for. A real backend would do here too, but only a double reaches `_spawn` with an instant
    process."""

    name = "nobarrier"
    capabilities = SimpleNamespace(
        chooses_session_id=False,
        network_control=SimpleNamespace(can_enable=tuple(FREEDOMS), can_block=()),
    )

    def enforcement(self, freedom, network=None):
        return Enforcement(
            freedom=freedom,
            mechanism="none",
            os_enforced=False,
            writes_confined=False,
            network_access="not_controlled",
            caveats=("test double",),
        )


async def test_a_dispatch_below_publish_pays_no_thread_hop_for_the_branch_check(
    git_repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The freedom gate is on the call site, not only inside the helper.

    Checking it only inside the helper still costs an executor round trip on every dispatch, and
    the helper's own deadline does not start until a worker picks the work up — so a saturated
    executor would delay a task that has no use for the check at all.
    """
    hops: list[object] = []
    real = asyncio.to_thread

    async def counting_to_thread(func, /, *args, **kwargs):
        hops.append(func)
        return await real(func, *args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", counting_to_thread)

    registry = TaskRegistry(log_dir=tmp_path / "streams")
    task = await registry.start("x", git_repo, backend=_TrivialBackend(), freedom="write_in_repo")
    await task.done.wait()

    assert tasks_module._publish_branch_notice not in hops
    assert task.bridge_notices == []


def test_a_remote_head_pointing_at_a_deleted_branch_is_not_trusted(tmp_path: Path) -> None:
    """A recorded `origin/HEAD` outlives the branch it names — the local ref file is not cleaned up.

    Warning "you are on the repository's default branch" on the strength of a dangling pointer would
    be a claim about a branch that no longer exists.
    """
    repo = _repo_with_remote(tmp_path, branch="main")
    # Point origin/HEAD at a branch that was never created, and stand on a branch of that name.
    (repo / ".git" / "refs" / "remotes" / "origin" / "HEAD").write_text(
        "ref: refs/remotes/origin/gone\n"
    )
    subprocess.run(["git", "-C", str(repo), "checkout", "-q", "-b", "gone"], check=True)

    notice = tasks_module._publish_branch_notice("publish", repo, _codex_enforcement("publish"))

    assert notice is not None
    assert "could not be determined" in notice
    assert "'gone'" not in notice


@pytest.mark.parametrize(
    "boom",
    [FileNotFoundError("git"), subprocess.TimeoutExpired("git", 3.0), RuntimeError("unexpected")],
    ids=["git-missing", "timeout", "unexpected"],
)
def test_a_broken_git_never_stops_a_dispatch(
    tmp_path: Path, boom: Exception, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CLAUDE.md invariant: bookkeeping must never change an outcome.

    A stale attribute in a *log line* once turned a successful run into `failed`, so every failure
    mode here has to be absorbed into the notice rather than escape into the dispatch.
    """
    repo = _repo_with_remote(tmp_path, branch="main")

    def explode(*args, **kwargs):
        raise boom

    monkeypatch.setattr(tasks_module.subprocess, "run", explode)

    notice = tasks_module._publish_branch_notice("publish", repo, _codex_enforcement("publish"))

    assert notice is not None
    assert "could not be determined" in notice


async def test_resume_inherits_the_parents_network_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`None` means "no explicit request", which on a resume has to mean the parent's, not the
    freedom's default — otherwise a continuation of a network-enabled run would silently lose it.

    Asserts on the argv, not just the metadata kwarg: a resume that reported `network` while
    building an argv without the `-c` pair would pass a weaker check.
    """
    registry = TaskRegistry(log_dir=tmp_path)
    parent = make_task(tmp_path, "parent", session_id="s1", finished=True)
    parent.backend = "codex"
    parent.freedom = "write_in_repo"
    parent.network = True
    registry._tasks[parent.task_id] = parent

    captured: dict = {}

    async def fake_spawn(argv, **kwargs):
        captured["argv"] = argv
        captured.update(kwargs)
        return make_task(tmp_path, "child", session_id="s1")

    monkeypatch.setattr(registry, "_spawn", fake_spawn)

    await registry.resume(parent, "carry on")

    assert captured["network"] is True
    assert "sandbox_workspace_write.network_access=true" in captured["argv"]


async def test_an_explicit_network_on_resume_overrides_the_parents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Network is a per-run sandbox setting, so changing it on a continuation cannot misrepresent
    the reply the parent already gave — unlike model or freedom, which are inherited outright."""
    registry = TaskRegistry(log_dir=tmp_path)
    parent = make_task(tmp_path, "parent", session_id="s1", finished=True)
    parent.backend = "codex"
    parent.freedom = "write_in_repo"
    parent.network = True
    registry._tasks[parent.task_id] = parent

    captured: dict = {}

    async def fake_spawn(argv, **kwargs):
        captured["argv"] = argv
        captured.update(kwargs)
        return make_task(tmp_path, "child", session_id="s1")

    monkeypatch.setattr(registry, "_spawn", fake_spawn)

    await registry.resume(parent, "carry on", network=False)

    assert captured["network"] is False
    assert "sandbox_workspace_write.network_access=false" in captured["argv"]


async def test_resume_record_inherits_the_recorded_network_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same carry-over through the path recovering a task this process never spawned."""
    registry = TaskRegistry(log_dir=tmp_path)
    record = store.TaskRecord(
        task_id="old",
        backend="codex",
        session_id="s1",
        markers=["s1"],
        repo_path=str(tmp_path),
        started_at=datetime.now(timezone.utc).isoformat(),
        status="completed",
        freedom="write_in_repo",
        network=True,
    )

    captured: dict = {}

    async def fake_spawn(argv, **kwargs):
        captured["argv"] = argv
        captured.update(kwargs)
        return make_task(tmp_path, "child", session_id="s1")

    monkeypatch.setattr(registry, "_spawn", fake_spawn)

    await registry.resume_record(record, "carry on")

    assert captured["network"] is True
    assert "sandbox_workspace_write.network_access=true" in captured["argv"]


async def test_a_pre_change_record_resumes_at_the_freedoms_historical_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A record written before this parameter existed deserializes `network` to None, and must
    resume exactly as it always did — network blocked at write_in_repo."""
    registry = TaskRegistry(log_dir=tmp_path)
    record = store.TaskRecord(
        task_id="old",
        backend="codex",
        session_id="s1",
        markers=["s1"],
        repo_path=str(tmp_path),
        started_at=datetime.now(timezone.utc).isoformat(),
        status="completed",
        freedom="write_in_repo",
    )

    captured: dict = {}

    async def fake_spawn(argv, **kwargs):
        captured["argv"] = argv
        captured.update(kwargs)
        return make_task(tmp_path, "child", session_id="s1")

    monkeypatch.setattr(registry, "_spawn", fake_spawn)

    await registry.resume_record(record, "carry on")

    assert captured["network"] is None
    assert "sandbox_workspace_write.network_access=false" in captured["argv"]


async def test_resume_refuses_a_network_request_the_parents_freedom_cannot_honour(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An override has to fail as loudly on resume as it would on start_task — silently dropping
    it is the failure mode the parameter exists to avoid."""
    registry = TaskRegistry(log_dir=tmp_path)
    parent = make_task(tmp_path, "parent", session_id="s1", finished=True)
    parent.backend = "codex"
    parent.freedom = "read_only"
    registry._tasks[parent.task_id] = parent

    async def fake_spawn(argv, **kwargs):
        raise AssertionError("must not reach spawn")

    monkeypatch.setattr(registry, "_spawn", fake_spawn)

    with pytest.raises(backends.UnsupportedCapability, match="cannot enable network"):
        await registry.resume(parent, "carry on", network=True)


async def test_resume_record_honours_an_explicit_network_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The recovered path needs the override half too, not just inheritance — a resume of a task
    this process never spawned is exactly where a silently-ignored override would go unnoticed."""
    registry = TaskRegistry(log_dir=tmp_path)
    record = store.TaskRecord(
        task_id="old",
        backend="codex",
        session_id="s1",
        markers=["s1"],
        repo_path=str(tmp_path),
        started_at=datetime.now(timezone.utc).isoformat(),
        status="completed",
        freedom="write_in_repo",
        network=True,
    )

    captured: dict = {}

    async def fake_spawn(argv, **kwargs):
        captured["argv"] = argv
        captured.update(kwargs)
        return make_task(tmp_path, "child", session_id="s1")

    monkeypatch.setattr(registry, "_spawn", fake_spawn)

    await registry.resume_record(record, "carry on", network=False)

    assert captured["network"] is False
    assert "sandbox_workspace_write.network_access=false" in captured["argv"]


async def test_resume_record_refuses_an_override_the_recorded_freedom_cannot_honour(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same loud refusal as the live-parent path — reached through the recovered record."""
    registry = TaskRegistry(log_dir=tmp_path)
    record = store.TaskRecord(
        task_id="old",
        backend="codex",
        session_id="s1",
        markers=["s1"],
        repo_path=str(tmp_path),
        started_at=datetime.now(timezone.utc).isoformat(),
        status="completed",
        freedom="read_only",
    )

    async def fake_spawn(argv, **kwargs):
        raise AssertionError("must not reach spawn")

    monkeypatch.setattr(registry, "_spawn", fake_spawn)

    with pytest.raises(backends.UnsupportedCapability, match="cannot enable network"):
        await registry.resume_record(record, "carry on", network=True)


async def test_a_network_request_with_no_barrier_is_disclosed_and_persisted(
    git_repo: Path, tmp_path: Path
) -> None:
    """Driven through the real `_spawn`, not by assigning the notice by hand.

    The first version of this test invented the notice string itself and then asserted that
    persistence kept it — which would have passed with the production notice generation deleted
    outright, and duplicated a round trip test_store.py already owns. Here the notice has to be
    produced by the dispatch to exist at all.
    """
    registry = TaskRegistry(log_dir=tmp_path)

    task = await registry.start(
        "x", git_repo, backend=_NoBarrierBackend(), freedom="write_in_repo", network=True
    )
    await task.done.wait()

    assert any("no network barrier" in n for n in task.bridge_notices), task.bridge_notices
    record = store.read(tmp_path, task.task_id)
    assert record is not None
    assert record.network is True
    assert any("no network barrier" in n for n in record.bridge_notices)


async def test_no_such_notice_when_nothing_was_asked(git_repo: Path, tmp_path: Path) -> None:
    """The other direction: omitting `network` must not manufacture a disclosure about it."""
    registry = TaskRegistry(log_dir=tmp_path)

    task = await registry.start("x", git_repo, backend=_NoBarrierBackend(), freedom="write_in_repo")
    await task.done.wait()

    assert not any("network barrier" in n for n in task.bridge_notices), task.bridge_notices


async def test_the_enforcement_reported_and_persisted_reflects_the_network_request(
    git_repo: Path, tmp_path: Path
) -> None:
    """`_spawn` computes Enforcement once, from the same (freedom, network) pair it built the argv
    from, so the block a caller reads describes the run it actually got.

    Guards a mutation the rest of the suite missed: computing it as `enforcement(freedom, None)`
    would report `network_access: "blocked"` for a run whose argv enables the network — the
    payload contradicting the process, which is precisely the overclaim this repo forbids.
    """
    registry = TaskRegistry(log_dir=tmp_path)

    task = await registry.start(
        "x", git_repo, backend=_NetworkAwareBackend(), freedom="write_in_repo", network=True
    )
    await task.done.wait()

    assert task.enforcement["network_access"] == "enabled"
    record = store.read(tmp_path, task.task_id)
    assert record is not None
    assert record.enforcement["network_access"] == "enabled"
