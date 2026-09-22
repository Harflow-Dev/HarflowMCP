"""Tool surface: validation, capability errors, and the backend parameter."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest
from mcp import Client, MCPError

from polybridge import server, store
from polybridge.tasks import Task


async def call(tool: str, **arguments):
    """Call one tool on an in-memory client.

    An MCPError is captured and re-raised after the client has closed: letting it escape the
    `async with` gets it wrapped in an anyio ExceptionGroup, which pytest.raises would not match.
    """
    error: MCPError | None = None
    async with Client(server.mcp) as client:
        try:
            return await client.call_tool(tool, arguments)
        except MCPError as exc:
            error = exc
    raise error


@pytest.fixture
def fake_task(tmp_path: Path) -> Task:
    task = Task(
        task_id="task-1",
        backend="claude",
        session_id="session-1",
        repo_path=tmp_path,
        prompt="x",
        max_turns=None,
        log_path=tmp_path / "task-1.jsonl",
        started_at=datetime.now(timezone.utc),
    )
    server._reg()._tasks[task.task_id] = task
    return task


async def test_all_seven_tools_are_exposed() -> None:
    async with Client(server.mcp) as client:
        names = sorted(tool.name for tool in (await client.list_tools()).tools)

    assert names == [
        "cancel_task",
        "get_task_status",
        "list_backends",
        "list_tasks",
        "resume_task",
        "start_task",
        "wait_for_task",
    ]


async def test_list_backends_describes_every_one() -> None:
    listed = (await call("list_backends")).structured_content["result"]

    by_name = {entry["backend"]: entry for entry in listed}
    assert sorted(by_name) == ["claude", "codex", "opencode", "vibe"]
    # The differences a caller has to plan around.
    assert by_name["claude"]["capabilities"]["supports_turn_cap"] is True
    assert by_name["codex"]["capabilities"]["supports_turn_cap"] is False
    assert by_name["codex"]["capabilities"]["os_sandbox"] is True
    assert by_name["opencode"]["capabilities"]["supports_turn_cap"] is False
    # Only codex has an OS boundary; opencode's restrictions are the agent's own.
    assert by_name["opencode"]["capabilities"]["os_sandbox"] is False
    assert by_name["opencode"]["capabilities"]["reports_cost_usd"] is True
    assert by_name["claude"]["capabilities"]["os_sandbox"] is False
    # vibe is the one backend with no model-selection flag at all, and no reasoning effort control.
    assert by_name["vibe"]["capabilities"]["supports_model_selection"] is False
    assert by_name["claude"]["capabilities"]["supports_model_selection"] is True
    assert by_name["codex"]["capabilities"]["supports_model_selection"] is True
    assert by_name["opencode"]["capabilities"]["supports_model_selection"] is True
    assert by_name["vibe"]["capabilities"]["reasoning_effort"]["accepts_parameter"] is False
    # But vibe genuinely does support a turn cap — unlike codex and opencode.
    assert by_name["vibe"]["capabilities"]["supports_turn_cap"] is True


async def test_unknown_backend_is_rejected(git_repo: Path) -> None:
    with pytest.raises(MCPError, match="unknown backend"):
        await call("start_task", prompt="x", repo_path=str(git_repo), backend="gemini")


async def test_unknown_freedom_is_rejected(git_repo: Path) -> None:
    with pytest.raises(MCPError, match="unknown freedom"):
        await call("start_task", prompt="x", repo_path=str(git_repo), freedom="yolo")


async def test_turn_cap_on_codex_fails_rather_than_being_ignored(git_repo: Path) -> None:
    """Dropping it silently would leave the caller believing a cap was applied."""
    with pytest.raises(MCPError, match="no turn cap"):
        await call(
            "start_task", prompt="x", repo_path=str(git_repo), backend="codex", max_turns=5
        )


async def test_model_on_vibe_fails_rather_than_being_ignored(git_repo: Path) -> None:
    """vibe has no model-selection flag at all — a silent no-op is exactly what this repo forbids."""
    with pytest.raises(MCPError, match="no model selection flag"):
        await call(
            "start_task", prompt="x", repo_path=str(git_repo), backend="vibe",
            model="mistral-medium",
        )
    assert (await call("list_tasks")).structured_content["result"] == []


async def test_model_on_vibe_resume_surfaces_as_invalid_params_not_an_internal_error(
    fake_task: Task,
) -> None:
    """Exercises the resume_task except block: build_resume_argv raises UnsupportedCapability from
    inside the argv builder, which must surface as INVALID_PARAMS, not an unhandled tool failure."""
    fake_task.backend = "vibe"
    fake_task.model = "mistral-medium"
    fake_task.status = "completed"
    fake_task.done.set()

    with pytest.raises(MCPError, match="no model selection flag"):
        await call("resume_task", task_id=fake_task.task_id, followup_prompt="carry on")


async def test_invalid_reasoning_effort_fails_before_a_task_exists(git_repo: Path) -> None:
    """Not just a backend-specific quirk: an out-of-vocabulary value is rejected up front."""
    with pytest.raises(MCPError, match="unknown reasoning_effort"):
        await call("start_task", prompt="x", repo_path=str(git_repo), reasoning_effort="ultra")
    assert (await call("list_tasks")).structured_content["result"] == []


async def test_valid_reasoning_effort_reaches_the_argv_and_the_persisted_record(
    git_repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Not just accepted — proven to reach both the built subprocess argv and what is written to
    disk, not only an in-memory kwarg a weaker assertion could pass without either one being true."""
    captured: dict = {}

    async def fake_spawn(argv, **kwargs):
        captured["argv"] = argv
        captured.update(kwargs)
        task = Task(
            task_id="fake-task",
            backend=kwargs["backend"].name,
            session_id=kwargs.get("session_id"),
            repo_path=kwargs["repo_path"],
            prompt=kwargs["prompt"],
            max_turns=kwargs.get("max_turns"),
            log_path=kwargs["repo_path"] / "fake-task.jsonl",
            started_at=datetime.now(timezone.utc),
            model=kwargs.get("model"),
            reasoning_effort=kwargs.get("reasoning_effort"),
            freedom=kwargs.get("freedom", "write_in_repo"),
        )
        registry = server._reg()
        registry._tasks[task.task_id] = task
        registry.persist(task)
        return task

    monkeypatch.setattr(server._reg(), "_spawn", fake_spawn)

    result = (
        await call(
            "start_task", prompt="x", repo_path=str(git_repo), backend="claude",
            reasoning_effort="high",
        )
    ).structured_content

    argv = captured["argv"]
    assert argv[argv.index("--effort") + 1] == "high"

    record = store.read(server._reg().log_dir, result["task_id"])
    assert record is not None
    assert record.reasoning_effort == "high"


async def test_rejects_a_directory_that_is_not_a_git_repo(tmp_path: Path) -> None:
    plain = tmp_path / "not-a-repo"
    plain.mkdir()
    with pytest.raises(MCPError, match="not inside a git repository"):
        await call("start_task", prompt="do a thing", repo_path=str(plain))


async def test_rejects_a_path_that_does_not_exist(tmp_path: Path) -> None:
    with pytest.raises(MCPError, match="does not exist"):
        await call("start_task", prompt="x", repo_path=str(tmp_path / "nope"))


@pytest.mark.parametrize("prompt", ["", "   "])
async def test_rejects_an_empty_prompt(git_repo: Path, prompt: str) -> None:
    with pytest.raises(MCPError, match="non-empty"):
        await call("start_task", prompt=prompt, repo_path=str(git_repo))


@pytest.mark.parametrize(
    ("tool", "arguments"),
    [
        ("get_task_status", {}),
        ("wait_for_task", {"timeout_seconds": 1}),
        ("cancel_task", {}),
        ("resume_task", {"followup_prompt": "more"}),
    ],
)
async def test_unknown_task_id_is_rejected(tool: str, arguments: dict) -> None:
    with pytest.raises(MCPError, match="unknown task_id"):
        await call(tool, task_id="no-such-task", **arguments)


async def test_rejects_an_unknown_status_filter() -> None:
    with pytest.raises(MCPError, match="unknown status"):
        await call("list_tasks", status="sleeping")


async def test_rejects_an_unknown_backend_filter() -> None:
    with pytest.raises(MCPError, match="unknown backend"):
        await call("list_tasks", backend="nope")


async def test_listing_can_filter_by_backend(fake_task: Task) -> None:
    assert (await call("list_tasks", backend="claude")).structured_content["result"]
    assert (await call("list_tasks", backend="codex")).structured_content["result"] == []


async def test_cannot_resume_a_task_that_is_still_running(fake_task: Task) -> None:
    with pytest.raises(MCPError, match="still running"):
        await call("resume_task", task_id=fake_task.task_id, followup_prompt="carry on")


async def test_cannot_resume_a_task_with_no_session_id(fake_task: Task) -> None:
    """Codex only discloses its id mid-run, so a run that died early cannot be continued."""
    fake_task.session_id = None
    fake_task.backend = "codex"
    fake_task.status = "failed"
    fake_task.done.set()

    with pytest.raises(MCPError, match="never disclosed a session id"):
        await call("resume_task", task_id=fake_task.task_id, followup_prompt="carry on")


async def test_default_wait_stays_under_a_typical_client_request_timeout() -> None:
    """MCP clients time out requests around 60s, reporting -32001 while the task runs on."""
    assert server.DEFAULT_WAIT_SECONDS < 60


async def test_wait_tells_the_caller_what_to_do_when_still_running(
    fake_task: Task, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server, "PROGRESS_INTERVAL_SECONDS", 0.01)

    snapshot = (
        await call("wait_for_task", task_id=fake_task.task_id, timeout_seconds=1)
    ).structured_content

    assert snapshot["status"] == "running"
    assert "get_task_status" in snapshot["next_step"]


async def test_a_started_task_reports_its_enforcement(fake_task: Task) -> None:
    """The caller should never have to infer what the freedom parameter actually bought."""
    status = (await call("get_task_status", task_id=fake_task.task_id)).structured_content

    assert status["backend"] == "claude"
    assert "enforcement" in status


class _RecordingContext:
    """Stands in for the MCP Context, capturing the progress reports a wait emits."""

    def __init__(self) -> None:
        self.reports: list[tuple[float, float | None, str | None]] = []

    async def report_progress(self, progress, total=None, message=None) -> None:
        self.reports.append((progress, total, message))


async def test_waiting_on_a_recovered_task_does_not_believe_a_stale_terminal_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A `failed` record about a live process must not end the wait after one interval.

    Breaking on the record's own status returned in a single tick while `next_step` claimed the
    whole timeout had elapsed — a wait that looked like it had done its job and had not.
    """
    monkeypatch.setattr(server, "PROGRESS_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: True)
    record = store.TaskRecord(
        task_id="orphan",
        backend="claude",
        session_id="session-1",
        markers=["session-1"],
        repo_path="/tmp",
        started_at=datetime.now(timezone.utc).isoformat(),
        pid=4321,
        pgid=4321,
        status="failed",  # what a torn-down server wrote about a run that kept going
        exit_code=None,
    )
    store.write(server._reg().log_dir, record)

    ctx = _RecordingContext()
    await server._poll_recovered(record, 1, ctx)

    assert len(ctx.reports) > 1, "the wait ended early instead of polling for its full timeout"


async def test_waiting_on_a_recovered_task_stops_once_it_has_really_settled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(server, "PROGRESS_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: False)
    record = store.TaskRecord(
        task_id="settled",
        backend="claude",
        session_id="session-1",
        markers=["session-1"],
        repo_path="/tmp",
        started_at=datetime.now(timezone.utc).isoformat(),
        pid=4321,
        status="completed",
        exit_code=0,
    )
    store.write(server._reg().log_dir, record)

    ctx = _RecordingContext()
    await asyncio.wait_for(server._poll_recovered(record, 30, ctx), timeout=2)

    assert ctx.reports == []


@pytest.mark.parametrize("value", ["true", "yes", "on", 1, 0])
async def test_a_non_boolean_network_is_refused_rather_than_coerced(value) -> None:
    """Truthiness must not decide a sandbox setting.

    This is the reason the two tool signatures say `StrictBool | None` and not `bool | None`:
    pydantic's lax mode coerces every value below into a real boolean (measured directly —
    `TypeAdapter(bool | None)` maps "yes"/"true"/"on"/1 to True and 0 to False), so with a plain
    `bool` the server's own isinstance guard never sees the string and a malformed request
    silently becomes a network decision the caller never made. Worse, a coerced `False` would be
    indistinguishable from omitting the parameter, which means something different.

    Asserted on the tool result rather than with `pytest.raises`: argument-binding failures come
    back as an error result, not an MCPError, so a `raises` test here passes vacuously.
    """
    async with Client(server.mcp) as client:
        result = await client.call_tool(
            "start_task",
            {"prompt": "x", "repo_path": "/nonexistent", "backend": "codex", "network": value},
        )

    assert result.is_error
    assert "valid boolean" in str(result.content)


async def test_network_true_on_codex_read_only_fails_rather_than_being_ignored(
    git_repo: Path,
) -> None:
    """Measured: codex's read-only sandbox is immune to the network switch in both directions,
    so honouring this request is impossible and silently dropping it would be the lie this repo
    exists to prevent."""
    with pytest.raises(MCPError, match="cannot enable network"):
        await call(
            "start_task",
            prompt="x",
            repo_path=str(git_repo),
            backend="codex",
            freedom="read_only",
            network=True,
        )


async def test_network_false_on_a_backend_with_no_barrier_fails_loudly(git_repo: Path) -> None:
    """claude has no sandbox at all, so it cannot impose the barrier `False` asks for. Accepting
    it and reporting `not_controlled` would answer a request with "we don't know"."""
    with pytest.raises(MCPError, match="no network barrier"):
        await call(
            "start_task",
            prompt="x",
            repo_path=str(git_repo),
            backend="claude",
            network=False,
        )


async def test_resume_task_also_refuses_a_non_boolean_network() -> None:
    """The parameter exists on both entry points, so the strictness has to as well — and this
    would regress silently if only `start_task` carried `StrictBool`."""
    async with Client(server.mcp) as client:
        result = await client.call_tool(
            "resume_task", {"task_id": "nope", "followup_prompt": "x", "network": "true"}
        )

    assert result.is_error
    assert "valid boolean" in str(result.content)


async def test_a_valid_network_request_reaches_the_argv_enforcement_and_the_record(
    git_repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The positive half the refusal tests cannot give.

    Deleting `network=network` from start_task's registry call would leave every other new server
    test green while valid requests silently reverted to the freedom's historical default — the
    "passes with the feature reverted" failure this repo has been burned by. So this asserts the
    request reached all three places a caller can observe: the subprocess argv, the enforcement
    block reported back, and the persisted record a later process resumes from.
    """
    captured: dict = {}

    async def fake_spawn(argv, **kwargs):
        captured["argv"] = argv
        captured.update(kwargs)
        task = Task(
            task_id="fake-task",
            backend=kwargs["backend"].name,
            session_id=kwargs.get("session_id"),
            repo_path=kwargs["repo_path"],
            prompt=kwargs["prompt"],
            max_turns=kwargs.get("max_turns"),
            log_path=kwargs["repo_path"] / "fake-task.jsonl",
            started_at=datetime.now(timezone.utc),
            model=kwargs.get("model"),
            reasoning_effort=kwargs.get("reasoning_effort"),
            freedom=kwargs.get("freedom", "write_in_repo"),
            network=kwargs.get("network"),
            enforcement=kwargs["backend"]
            .enforcement(kwargs.get("freedom", "write_in_repo"), kwargs.get("network"))
            .as_dict(),
        )
        registry = server._reg()
        registry._tasks[task.task_id] = task
        registry.persist(task)
        return task

    monkeypatch.setattr(server._reg(), "_spawn", fake_spawn)

    result = (
        await call(
            "start_task", prompt="x", repo_path=str(git_repo), backend="codex",
            freedom="write_in_repo", network=True,
        )
    ).structured_content

    assert "sandbox_workspace_write.network_access=true" in captured["argv"]
    assert result["enforcement"]["network_access"] == "enabled"

    record = store.read(server._reg().log_dir, result["task_id"])
    assert record is not None
    assert record.network is True


@pytest.mark.parametrize("path", ["live-parent", "recovered-record"])
async def test_resume_forwards_network_through_both_server_branches(
    git_repo: Path, monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    """`resume_task` has two independent dispatch branches — a parent still in this process's
    registry, and a record recovered from disk — each with its own `network=` forwarding. The
    registry-level tests call `resume`/`resume_record` directly, so changing either server call to
    forward `network=None` would leave them green while the tool silently dropped the override.
    """
    captured: dict = {}

    async def fake_spawn(argv, **kwargs):
        captured["argv"] = argv
        captured.update(kwargs)
        task = Task(
            task_id="resumed",
            backend=kwargs["backend"].name,
            session_id=kwargs.get("session_id"),
            repo_path=kwargs["repo_path"],
            prompt=kwargs["prompt"],
            max_turns=kwargs.get("max_turns"),
            log_path=kwargs["repo_path"] / "resumed.jsonl",
            started_at=datetime.now(timezone.utc),
            freedom=kwargs.get("freedom", "write_in_repo"),
            network=kwargs.get("network"),
        )
        server._reg()._tasks[task.task_id] = task
        return task

    registry = server._reg()
    monkeypatch.setattr(registry, "_spawn", fake_spawn)

    if path == "live-parent":
        parent = Task(
            task_id="parent-1",
            backend="codex",
            session_id="thread-1",
            repo_path=git_repo,
            prompt="x",
            max_turns=None,
            log_path=git_repo / "parent-1.jsonl",
            started_at=datetime.now(timezone.utc),
            freedom="write_in_repo",
        )
        parent.status = "completed"
        parent.done.set()
        registry._tasks[parent.task_id] = parent
        task_id = parent.task_id
    else:
        record = store.TaskRecord(
            task_id="recovered-1",
            backend="codex",
            session_id="thread-2",
            markers=["thread-2"],
            repo_path=str(git_repo),
            started_at=datetime.now(timezone.utc).isoformat(),
            status="completed",
            freedom="write_in_repo",
        )
        store.write(registry.log_dir, record)
        registry._tasks.pop("recovered-1", None)
        task_id = record.task_id

    await call("resume_task", task_id=task_id, followup_prompt="carry on", network=True)

    assert captured["network"] is True
    assert "sandbox_workspace_write.network_access=true" in captured["argv"]
