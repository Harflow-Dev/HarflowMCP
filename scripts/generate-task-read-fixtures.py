#!/usr/bin/env python3
"""Regenerate the task-read fixtures in contracts/v1 from actual `harflow-ctl` output.

    uv run python scripts/generate-task-read-fixtures.py            # rewrite contracts/v1
    uv run python scripts/generate-task-read-fixtures.py --check    # fail if contracts/v1 differs from fresh, isolated runs

Owns only the fixtures named by the prefixes in `PREFIXES` (task events, task prompts, successor discovery,
live workflow node activity and statistics cost), plus their manifest entries; every other
file named in the manifest is verified against its recorded hash and left alone. Each document is the output of
`ctl.main` against an isolated HOME and state root. Event logs are written by the production `EventLog` writer;
task records by `store.write`. Controlled facts: the wall clock (event stamps, `observed_at`, the settling
grace), the catalog identity that task-page cursors bind to, the statistics clock and source witnesses (inode and ctime, for the statistics fixture only), process liveness (`identity.identity_check`, `store.record_process_alive`, `identity.own_identity`), the file identity cursors bind to (device and inode) and
the record file's mtime, none of which can be reproduced across machines. The temporary base directory never
appears in a document.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "contracts" / "v1"
EVENTS_CAPABILITY, PROMPT_CAPABILITY = "task-event-page.v1", "task-prompt.v1"
SUCCESSORS_CAPABILITY = "task-successors.v1"
LIVE_ACTIVITY_CAPABILITY = "workflow-live-activity.v1"
COST_MODEL_CAPABILITY = "statistics-cost-model.v1"
# The fixture-name prefix each capability's documents are written under.
PREFIXES = {
    EVENTS_CAPABILITY: "ctl-task-events-",
    PROMPT_CAPABILITY: "ctl-task-prompt-",
    SUCCESSORS_CAPABILITY: "ctl-task-page-successors-",
    LIVE_ACTIVITY_CAPABILITY: "ctl-workflow-inspect-live-",
    COST_MODEL_CAPABILITY: "ctl-statistics-page-cost-",
}
EPOCH = 1_792_000_000.0
OWNER = {"pid": 5678, "start_time": "bridge-start", "markers": []}
WORKSPACE = "/workspace/repo"


def canonical_bytes(document) -> bytes:
    return (json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")


def manifest_bytes(manifest: dict) -> bytes:
    return (json.dumps(manifest, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


class World:
    def __init__(self, base: Path) -> None:
        self.base = Path(os.path.realpath(base))
        self.now = EPOCH
        self.documents: dict[str, dict] = {}

    # --- environment ------------------------------------------------------------------------------------

    def enter(self, stack: contextlib.ExitStack) -> None:
        from harflow_mcp import ctl, event_pages, events, identity, lineage, state_ownership, store, takeover, task_event_read
        from harflow_mcp.state_layout import resolve_layout
        self.ctl, self.events, self.store, self.takeover, self.lineage = ctl, events, store, takeover, lineage
        self.real_refusal = takeover.caller_refusal
        self.layout = resolve_layout(root=self.base / ".harflow")
        stack.enter_context(mock.patch.dict(os.environ, {"HOME": str(self.base), "TMPDIR": str(self.base)}))
        for key in ("HARFLOW_STATE_ROOT", "HARFLOW_TASK_ID", "HARFLOW_DEPTH"):
            os.environ.pop(key, None)
        stack.enter_context(state_ownership.state_session(self.layout))
        self.tasks = self.layout.tasks_directory
        self.tasks.mkdir(parents=True)
        stamp = lambda: datetime.fromtimestamp(self.now, timezone.utc)
        stack.enter_context(mock.patch.object(events, "_now_iso", lambda: stamp().isoformat()))
        stack.enter_context(mock.patch.object(task_event_read, "_now", stamp))
        stack.enter_context(mock.patch.object(event_pages, "identity_of", lambda info: (1, 1)))
        stack.enter_context(mock.patch.object(identity, "identity_check", lambda value: "alive" if value == OWNER else "undecidable"))
        from harflow_mcp import catalog
        init = catalog.Catalog.__init__

        def pinned_catalog(catalog_self, *args, **kwargs):
            init(catalog_self, *args, **kwargs)
            # Cursors are bound to a digest of the catalog directory, which is a temporary path here.
            catalog_self.identity = "fixture-catalog0"
        stack.enter_context(mock.patch.object(catalog.Catalog, "__init__", pinned_catalog))
        from harflow_mcp import statistics

        class FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.fromtimestamp(EPOCH, tz)
        stack.enter_context(mock.patch.object(statistics, "datetime", FixedDatetime))
        os.environ.pop("HARFLOW_RETENTION_DAYS", None)
        stack.enter_context(mock.patch.object(identity, "own_identity", lambda: {"pid": 4242, "start_time": "ctl-start", "markers": []}))
        stack.enter_context(mock.patch.object(store, "record_process_alive", lambda record: record.pid is not None))
        stack.enter_context(mock.patch.object(takeover, "caller_refusal", lambda *_: None))

    # --- seeding ----------------------------------------------------------------------------------------

    def iso(self, offset: float = 0.0) -> str:
        return datetime.fromtimestamp(EPOCH + offset, timezone.utc).isoformat()

    def task(self, task_id: str, *, status: str = "completed", drained: bool = True, finished: float | None = -60.0,
             prompt: str = "", title: str | None = None, started: float = -120.0, **extra) -> None:
        record = self.store.TaskRecord(
            task_id=task_id, backend="claude", session_id="session-" + task_id, markers=["session-" + task_id],
            repo_path=WORKSPACE, started_at=self.iso(started), status=status,
            exit_code=0 if status == "completed" else None, finished_at=None if finished is None else self.iso(finished),
            output_drained=drained, owner=OWNER, prompt=prompt, title=title, **extra)
        self.store.write(self.tasks, record)
        # The record's mtime is part of the change token; pin it.
        os.utime(self.store.record_path(self.tasks, task_id), (EPOCH, EPOCH))

    def log(self, task_id: str, script, *, finish: bool = True) -> None:
        """Write the events with the production writer; `script(writer)` may add or skip events."""
        path = self.events.events_path(self.tasks, task_id)
        writer = self.events.EventLog(path, task_id, layout=self.layout)
        try:
            script(writer)
            if finish:
                self.now += 1
                writer.write("task_finished", {"status": "completed", "exit_code": 0})
        finally:
            writer.close()

    # --- invocation -------------------------------------------------------------------------------------

    def call(self, name: str | None, *argv: str, capability: str = EVENTS_CAPABILITY, undecidable: bool = False,
             agent_caller: bool = False) -> dict:
        """Run one ctl command, keep its document as fixture `name` (None: do not keep), and return its `result` or `error`."""
        self.now = EPOCH
        out, err = io.StringIO(), io.StringIO()
        with contextlib.ExitStack() as stack:
            if undecidable:
                # The production gate, over a process table that cannot answer, with no ambient environment.
                stack.enter_context(mock.patch.object(self.lineage, "detect_caller_detail",
                                                      lambda *a, **k: self.lineage.Detection(None, "the process table is unavailable")))
                stack.enter_context(mock.patch.object(self.takeover, "caller_refusal", lambda log_dir, environ=None: self.real_refusal(log_dir, {})))
            if agent_caller:
                stack.enter_context(mock.patch.object(self.takeover, "caller_refusal", lambda *_: ("agent_caller", "called from a task")))
            stack.enter_context(contextlib.redirect_stdout(out))
            stack.enter_context(contextlib.redirect_stderr(err))
            code = self.ctl.main([*argv, "--json"])
        lines = [line for line in out.getvalue().splitlines() if line.strip()]
        if len(lines) != 1:
            raise SystemExit(f"{name}: expected one document, got {len(lines)} (exit {code}): {err.getvalue()}")
        document = json.loads(lines[0])
        assert (code == 0) == ("result" in document), (name, code, document)
        if name is not None:
            self.documents[PREFIXES[capability] + name] = {"document": document, "capability": capability}
        return document.get("result") or document["error"]


def build(world: World) -> None:
    import base64
    text = lambda log, body: log.write("assistant_text", {"text": body}, raw_offset=None)

    def conversation(log):
        log.write("task_started", {"backend": "claude", "model": None})
        log.write("assistant_delta", {"text": "Looking"})
        log.write("assistant_delta", {"text": " at the code."})
        text(log, "Looking at the code.")
        log.write("tool_call", {"tool": "Read", "call_id": "call-1", "input": {"file_path": WORKSPACE + "/README.md"}})
        log.write("tool_result", {"call_id": "call-1", "output": "Readme body", "is_error": False})
        log.write("usage", {"input_tokens": 120, "output_tokens": 30})
        text(log, "Done.")

    world.task("task-events-final", prompt="Describe the repository.")
    world.log("task-events-final", conversation)
    newest = world.call("newest", "task-events", "task-events-final", "--limit", "4")
    older = world.call("older", "task-events", "task-events-final", "--cursor", newest["older_cursor"], "--limit", "4")
    world.call("oldest-forward", "task-events", "task-events-final", "--from", "oldest", "--limit", "3")
    first = world.call(None, "task-events", "task-events-final", "--from", "oldest", "--limit", "2")
    world.call("newer", "task-events", "task-events-final", "--cursor", first["newer_cursor"], "--limit", "3")
    world.call("newer-empty-final", "task-events", "task-events-final", "--cursor", newest["newer_cursor"])
    world.call("all-kinds", "task-events", "task-events-final", "--all-kinds", "--limit", "10")
    world.call("kinds-filtered", "task-events", "task-events-final", "--kinds", "tool_call,tool_result", "--limit", "10")
    # A well-formed cursor from another engine generation, as a Rust engine would have issued.
    foreign = json.loads(base64.urlsafe_b64decode(older["newer_cursor"]))
    foreign["gen"] = "rs-events-1"
    world.call("cursor-expired", "task-events", "task-events-final", "--cursor",
               base64.urlsafe_b64encode(json.dumps(foreign, separators=(",", ":")).encode()).decode())
    world.call("cursor-invalid", "task-events", "task-events-final", "--cursor", "not-a-cursor")
    world.call("unknown-task", "task-events", "task-events-missing")
    world.call("direct-caller-refused", "task-events", "task-events-final", undecidable=True)

    world.task("task-events-settling", drained=True, finished=-5.0)
    world.log("task-events-settling", conversation, finish=False)
    world.call("settling", "task-events", "task-events-settling", "--limit", "3")

    world.task("task-events-incomplete", drained=False, finished=-300.0)
    world.log("task-events-incomplete", conversation, finish=False)
    world.call("incomplete", "task-events", "task-events-incomplete", "--limit", "3")

    world.task("task-events-unavailable", finished=-300.0)
    world.call("unavailable", "task-events", "task-events-unavailable")

    world.task("task-events-open", status="running", drained=False, finished=None, pid=9999)
    world.log("task-events-open", conversation, finish=False)
    world.call("open", "task-events", "task-events-open", "--limit", "3")

    def damaged(log):
        log.write("task_started", {"backend": "claude", "model": None})
        text(log, "Before the loss.")
        log._seq += 2  # two events that were dropped before they reached the log
        text(log, "After the loss.")
        log._handle.write(b"{ this line is not json\n")
        text(log, "After the unreadable line.")
    world.task("task-events-damaged")
    world.log("task-events-damaged", damaged)
    world.call("gaps-skipped", "task-events", "task-events-damaged")

    def streaming(log):
        for index in range(6):
            log.write("assistant_delta", {"text": f"chunk {index} "})
        text(log, "chunk 0 chunk 1 chunk 2 chunk 3 chunk 4 chunk 5 ")
    world.task("task-events-streaming")
    world.log("task-events-streaming", streaming)
    world.call("default-filter-delta-seqs", "task-events", "task-events-streaming")

    world.task("task-events-pending", status="running", drained=False, finished=None)
    from harflow_mcp import id_resolution, task_resources, task_supervision
    with mock.patch.object(id_resolution, "resolve_task", lambda *a, **k: id_resolution.Resolution("task-events-pending", None)), \
            mock.patch.object(world.store, "read", lambda *a, **k: None), \
            mock.patch.object(task_supervision, "read_launch", lambda *a: {"state": "waiting_for_resources"}), \
            mock.patch.object(task_resources, "is_pending_kind", lambda receipt: True):
        world.call("pending-launch", "task-events", "task-events-pending")

    long_prompt = "Review the change.\n" + "  Keep this indentation and wording verbatim.\n" * 60
    world.task("task-prompt-available", prompt=long_prompt, title="Review the change")
    world.call("available", "task-prompt", "task-prompt-available", capability=PROMPT_CAPABILITY)
    world.call("truncated", "task-prompt", "task-prompt-available", "--max-bytes", "120", capability=PROMPT_CAPABILITY)
    world.task("task-prompt-preview", prompt=long_prompt, title="Review the change")
    next(world.tasks.glob("task-prompt-preview.prompt.*.txt")).unlink()
    world.call("preview-only", "task-prompt", "task-prompt-preview", capability=PROMPT_CAPABILITY)
    world.task("task-prompt-damaged", prompt=long_prompt, title="Review the change")
    next(world.tasks.glob("task-prompt-damaged.prompt.*.txt")).write_text("tampered")
    world.call("integrity-failed", "task-prompt", "task-prompt-damaged", capability=PROMPT_CAPABILITY)

    build_successors(world)
    build_live_activity(world)
    build_statistics_cost(world)


def build_successors(world: World) -> None:
    from harflow_mcp import id_resolution, task_resources, task_supervision
    cap = SUCCESSORS_CAPABILITY
    root = "task-succ-root"
    world.task(root, prompt="Start the work.")
    world.task("task-succ-resume", parent_task_id=root, started=-100.0)
    world.task("task-succ-continuation", continuation_of_task_id=root, chain_root_task_id=root)
    world.task("task-succ-unrelated")
    world.task("task-succ-leaf", prompt="No follow-up yet.")
    for _ in range(10):  # the first reads only build the bounded catalog
        if not world.call(None, "task-list-page", capability=cap).get("bootstrap_pending"):
            break
    else:
        raise SystemExit("the task catalog did not become ready")
    world.call("found", "task-list-page", "--predecessor-task-id", root, capability=cap)
    world.call("found-first-page", "task-list-page", "--predecessor-task-id", root, "--limit", "1", capability=cap)
    world.call("none-found", "task-list-page", "--predecessor-task-id", "task-succ-leaf", capability=cap)
    world.call("unknown-predecessor", "task-list-page", "--predecessor-task-id", "task-succ-missing", capability=cap)
    world.call("direct-caller-refused", "task-list-page", "--predecessor-task-id", root, capability=cap, undecidable=True)
    world.task("task-succ-pending", status="running", drained=False, finished=None)
    with mock.patch.object(id_resolution, "resolve_task", lambda *a, **k: id_resolution.Resolution("task-succ-pending", None)), \
            mock.patch.object(world.store, "read", lambda *a, **k: None), \
            mock.patch.object(task_supervision, "read_launch", lambda *a: {"state": "waiting_for_resources"}), \
            mock.patch.object(task_resources, "is_pending_kind", lambda receipt: True):
        world.call("pending-launch-predecessor", "task-list-page", "--predecessor-task-id", "task-succ-pending", capability=cap)


def build_live_activity(world: World) -> None:
    from harflow_mcp import workflows
    cap = LIVE_ACTIVITY_CAPABILITY
    run_id = "fixture-live-run"
    storage = workflows.WorkflowStore(root=world.base / ".harflow")
    definition = storage.save("live-graph", {"nodes": [{"id": "start", "type": "start"}, {"id": "end", "type": "end"}],
                                             "connections": [{"id": "finish", "source": "start", "target": "end"}]})
    storage.create_run(definition, "synthetic assignment", world.base, run_id=run_id)

    def attempt(task_id, status):
        return {"task_id": task_id, "status": status}

    def reserve(value):
        value.update(status="running", execution_contract="delegation", activations=[
            {"id": "live-node", "node_id": "implement", "role": "node", "status": "running", "created_at": 0.0,
             "tasks": [attempt("live-attempt-1", "running")]},
            {"id": "reserved-node", "node_id": "review", "role": "node", "status": "running", "created_at": 0.0,
             "tasks": [attempt("live-attempt-2", "reserved")]},
            {"id": "waiting-node", "node_id": "report", "role": "node", "status": "running", "created_at": 0.0, "tasks": []},
        ])
    storage.update_run(run_id, reserve, "fixture_seed")
    world.log("live-attempt-1", lambda log: (log.write("task_started", {"backend": "claude", "model": None}),
                                             log.write("assistant_text", {"text": "Working on it."}, raw_offset=None),
                                             log.write("usage", {"input_tokens": 12, "output_tokens": 3})), finish=False)
    world.call("running-attempt", "workflow-inspect", run_id, "live-node", "--view", "activity", capability=cap)
    world.call("running-attempt-after", "workflow-inspect", run_id, "live-node", "--view", "activity", "--after-seq", "1", capability=cap)
    world.call("reserved-attempt", "workflow-inspect", run_id, "reserved-node", "--view", "activity", capability=cap)
    world.call("no-attempt", "workflow-inspect", run_id, "waiting-node", "--view", "activity", capability=cap)
    world.call("agent-caller-settled-only", "workflow-inspect", run_id, "live-node", "--view", "activity", capability=cap, agent_caller=True)


def build_statistics_cost(world: World) -> None:
    import shutil
    cap = COST_MODEL_CAPABILITY
    # Statistics read every retained record: start from an empty store so the page covers only its own tasks.
    for entry in world.tasks.iterdir():
        entry.unlink() if entry.is_file() or entry.is_symlink() else shutil.rmtree(entry)
    for entry in (world.base / ".harflow").glob("workflow-*"):
        shutil.rmtree(entry)
    reported = {"model": "observed-model", "provenance": "harness_initialization_event"}
    world.task("cost-reported", model="requested-model", total_cost_usd=0.4321, observed_model=reported, started=-300.0)
    world.task("cost-silent", model="requested-model", started=-200.0)
    world.task("cost-mixed-model", model="requested-model", total_cost_usd=1.5, started=-100.0,
               observed_model={"model": None, "provenance": "harness_initialization_event_mixed"})
    # Source witnesses carry inode and ctime, which differ per machine; keep what a fixture can reproduce.
    from harflow_mcp import statistics_index
    patch = mock.patch.object(statistics_index, "witness", lambda info: [0, 0, info.st_mtime_ns, 0, info.st_size])
    patch.start()
    argv = ("statistics-page", "--since", world.iso(-86400.0), "--before", world.iso(86400.0), "--limit", "10")
    for _ in range(10):  # the first reads only prepare the bounded derivative index
        got = world.call(None, *argv, capability=cap)
        if got.get("state") == "ready":
            break
    else:
        raise SystemExit("the statistics index did not become ready")
    world.call("reported-and-silent", *argv, capability=cap)
    patch.stop()


def finalize(world: World) -> tuple[dict[str, dict], dict[str, str]]:
    documents, capabilities = {}, {}
    for key, entry in world.documents.items():
        name = f"{key}.json"
        documents[name], capabilities[name] = entry["document"], entry["capability"]
    base = str(world.base)
    for name, document in documents.items():
        if base in json.dumps(document):
            raise SystemExit(f"{name} leaks the temporary base directory")
    return documents, capabilities


def write_output(output: Path, documents: dict[str, dict], capabilities: dict[str, str]) -> None:
    from harflow_mcp.wire_contracts import CTL_FORMAT, PRODUCT_VERSION, validate_envelope
    output.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((FIXTURES / "manifest.json").read_bytes())
    owned = lambda name: name.startswith(tuple(PREFIXES.values()))
    for name in [n for n in manifest["cases"] if owned(n) and n not in documents]:
        manifest["cases"].pop(name)
        manifest["files"].pop(name, None)
    for stale in [p for p in output.glob("ctl-*.json") if owned(p.name) and p.name not in documents]:
        stale.unlink()
    written: dict[str, bytes] = {}
    for name, document in sorted(documents.items()):
        validate_envelope(document, CTL_FORMAT, live=True, expected_version=PRODUCT_VERSION)
        written[name] = canonical_bytes(document)
        manifest["cases"][name] = {"encoding": "json", "expected_product_version": PRODUCT_VERSION, "format": CTL_FORMAT,
                                   "required_capabilities": [capabilities[name]], "valid": True}
    for name, payload in written.items():
        (output / name).write_bytes(payload)
        manifest["files"][name] = hashlib.sha256(payload).hexdigest()
    for name, recorded in manifest["files"].items():
        if name not in written and name != "manifest.json":
            if hashlib.sha256((FIXTURES / name).read_bytes()).hexdigest() != recorded:
                raise SystemExit(f"{name} differs from its recorded hash; this generator does not own it")
    payload = manifest_bytes(manifest)
    (output / "manifest.json").write_bytes(payload)
    (output / "manifest.sha256").write_text(f"{hashlib.sha256(payload).hexdigest()}  manifest.json\n")


def generate(output: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="harflow-task-read-fixtures-") as base:
        world = World(Path(base))
        with contextlib.ExitStack() as stack:
            world.enter(stack)
            build(world)
        documents, capabilities = finalize(world)
    write_output(output, documents, capabilities)


def owned_files(directory: Path) -> dict[str, bytes]:
    names = {p.name for p in directory.glob("ctl-*.json") if p.name.startswith(tuple(PREFIXES.values()))}
    names |= {"manifest.json", "manifest.sha256"}
    return {name: (directory / name).read_bytes() for name in sorted(names) if (directory / name).exists()}


def check() -> int:
    """Generate twice in separate processes into temporary directories and compare both with contracts/v1."""
    with tempfile.TemporaryDirectory(prefix="harflow-task-read-check-") as scratch:
        outputs = []
        for index in (1, 2):
            target = Path(scratch) / f"run{index}"
            subprocess.run([sys.executable, str(Path(__file__).resolve()), "--output", str(target)], check=True)
            outputs.append(owned_files(target))
        committed = owned_files(FIXTURES)
    problems = []
    if outputs[0] != outputs[1]:
        problems.append("two fresh runs differ (non-deterministic): " + ", ".join(sorted(n for n in set(outputs[0]) | set(outputs[1]) if outputs[0].get(n) != outputs[1].get(n))))
    if outputs[0] != committed:
        problems.append("contracts/v1 differs from a fresh run: " + ", ".join(sorted(n for n in set(outputs[0]) | set(committed) if outputs[0].get(n) != committed.get(n))))
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, default=FIXTURES, help="directory to write (default: contracts/v1)")
    parser.add_argument("--check", action="store_true", help="verify contracts/v1 is exactly reproducible; writes nothing there")
    arguments = parser.parse_args()
    if arguments.check:
        return check()
    generate(arguments.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
