#!/usr/bin/env python3
"""A scripted stand-in for `codex` used by the app-server adapter tests; no model, no network.

Installed as an executable named `codex` on a test PATH. `codex --version` prints the version in
`FAKE_CODEX_VERSION` (default 0.163.0). `codex app-server ...` speaks the measured JSON-RPC subset on
stdio, driven by the scenario file named in `FAKE_CODEX_SCENARIO` and logging every client frame (and its
own argv) as JSON lines to `FAKE_CODEX_LOG`.

Scenario keys (all optional):
  user_agent_version / thread_cli_version   what initialize and the thread response report
  thread                                     deep-merged over the generated thread/start|resume result
  hooks                                      the `hooks` list of the hooks/list entry
  hooks_result                               replaces the whole hooks/list result
  errors                                     {"method": {"code": n, "message": text}} answered with a JSON-RPC error
  before_turn                                steps run after the thread/start|resume answer, before hooks/list
  turn                                       steps run after the turn/start response

Steps: {"notify": {...}}, {"request": {...}, "await": name}, {"complete": status, "error": ...},
{"sleep": seconds}, {"exit": code}, {"hold": true} (block until stdin EOF, then exit 0), {"raw": text}, {"spawn": true} (a tool process in a group of its own), {"linger": true|"ignore_term"} (ignore stdin EOF, optionally SIGTERM too),
{"replay": "<transcript.jsonl>", "home": "<replaces <HOME>>"} (recorded server-to-client frames after the
turn/start response, ids rewritten to this run's thread and turn). Strings "$thread", "$turn" and "$cwd"
inside step values are replaced.
"""
import copy
import json
import os
import sys
import time

THREAD_ID = "11111111-2222-7333-8444-555555555555"
TURN_ID = "99999999-8888-7777-6666-555555555555"


def main() -> int:
    version = os.environ.get("FAKE_CODEX_VERSION", "0.163.0")
    if sys.argv[1:] == ["--version"]:
        print(f"codex-cli {version}")
        return 0
    scenario = json.load(open(os.environ["FAKE_CODEX_SCENARIO"])) if os.environ.get("FAKE_CODEX_SCENARIO") else {}
    log_path = os.environ.get("FAKE_CODEX_LOG")

    def log(entry):
        if log_path:
            with open(log_path, "a") as handle:
                handle.write(json.dumps(entry) + "\n")

    log({"argv": sys.argv[1:], "codex_home": os.environ.get("CODEX_HOME"), "cwd": os.getcwd()})
    if sys.argv[1:2] != ["app-server"]:
        print("fake codex only implements app-server", file=sys.stderr)
        return 2

    def send(frame):
        sys.stdout.write(json.dumps(frame) + "\n")
        sys.stdout.flush()

    state = {"cwd": os.getcwd(), "thread": THREAD_ID, "turn": TURN_ID}

    def sub(value):
        if isinstance(value, str):
            return value.replace("$thread", state["thread"]).replace("$turn", state["turn"]).replace("$cwd", state["cwd"])
        if isinstance(value, list):
            return [sub(v) for v in value]
        if isinstance(value, dict):
            return {k: sub(v) for k, v in value.items()}
        return value

    responses = {}

    def read():
        line = sys.stdin.readline()
        if not line:
            return None
        frame = json.loads(line)
        log(frame)
        return frame

    def await_response(wire_id):
        while True:
            frame = read()
            if frame is None:
                log({"eof": True})
                sys.exit(0)
            if "method" not in frame and frame.get("id") == wire_id and type(frame.get("id")) is type(wire_id):
                return frame

    def merge(base, extra):
        for key, value in extra.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                merge(base[key], value)
            else:
                base[key] = value
        return base

    def run(steps):
        for step in steps:
            if "notify" in step:
                send(sub(step["notify"]))
            elif "request" in step:
                request = sub(step["request"])
                send(request)
                if step.get("await"):
                    responses[step["await"]] = await_response(request["id"])
            elif "complete" in step:
                turn = {"id": state["turn"], "status": step["complete"], "error": step.get("error"), "items": []}
                send({"method": "turn/completed", "params": {"threadId": state["thread"], "turn": turn}})
            elif "sleep" in step:
                time.sleep(step["sleep"])
            elif "exit" in step:
                sys.stdout.flush()
                os._exit(step["exit"])
            elif "hold" in step:
                while read() is not None:
                    pass
                log({"eof": True})
                sys.exit(0)
            elif "linger" in step:
                import signal
                if step["linger"] == "ignore_term":
                    signal.signal(signal.SIGTERM, signal.SIG_IGN)
                while True:
                    time.sleep(0.2)
            elif "spawn" in step:
                import subprocess
                child = subprocess.Popen(["sleep", "120"], start_new_session=True)
                log({"spawned": child.pid, "pgid": os.getpgid(child.pid)})
            elif "raw" in step:
                sys.stdout.write(step["raw"] + "\n")
                sys.stdout.flush()
            elif "replay" in step:
                replay(step)

    def replay(step):
        rows = [json.loads(line) for line in open(step["replay"])]
        frames = [row["frame"] for row in rows if row["dir"] == "in" and isinstance(row["frame"], dict)]
        recorded_thread = next(f["result"]["thread"]["id"] for f in frames if "result" in f and isinstance(f["result"], dict) and "thread" in f["result"])
        turn_index = next(i for i, f in enumerate(frames) if isinstance(f.get("result"), dict) and "turn" in f["result"])
        recorded_turn = frames[turn_index]["result"]["turn"]["id"]
        for frame in frames[turn_index + 1:]:
            text = json.dumps(frame).replace(recorded_thread, state["thread"]).replace(recorded_turn, state["turn"]).replace("<HOME>", step.get("home", "<HOME>"))
            frame = json.loads(text)
            send(frame)
            if "method" in frame and "id" in frame:
                await_response(frame["id"])

    while True:
        frame = read()
        if frame is None:
            return 0
        method, wire_id = frame.get("method"), frame.get("id")
        if method is None or wire_id is None:
            continue
        params = frame.get("params") or {}
        error = (scenario.get("errors") or {}).get(method)
        if error is not None:
            send({"id": wire_id, "error": error})
            continue
        if method == "initialize":
            send({"id": wire_id, "result": {"userAgent": f"harflow/{scenario.get('user_agent_version', version)} (Mac OS 26.0; arm64) fake", "codexHome": os.environ.get("CODEX_HOME", ""), "platformFamily": "unix", "platformOs": "macos"}})
        elif method in {"thread/start", "thread/resume"}:
            state["cwd"] = params.get("cwd", state["cwd"])
            if method == "thread/resume":
                state["thread"] = params["threadId"]
            net = ((params.get("config") or {}).get("sandbox_workspace_write") or {}).get("network_access", False)
            result = {"thread": {"id": state["thread"], "cliVersion": scenario.get("thread_cli_version", version)}, "approvalPolicy": params.get("approvalPolicy"),
                      "approvalsReviewer": params.get("approvalsReviewer"), "cwd": state["cwd"], "runtimeWorkspaceRoots": [state["cwd"]],
                      "sandbox": {"type": "workspaceWrite", "writableRoots": [], "networkAccess": net, "excludeTmpdirEnvVar": False, "excludeSlashTmp": False}}
            send({"id": wire_id, "result": merge(result, copy.deepcopy(scenario.get("thread", {})))})
            run(scenario.get("before_turn", []))
        elif method == "hooks/list":
            result = scenario.get("hooks_result") or {"data": [{"cwd": state["cwd"], "hooks": scenario.get("hooks", []), "warnings": [], "errors": []}]}
            send({"id": wire_id, "result": result})
        elif method == "turn/start":
            send({"id": wire_id, "result": {"turn": {"id": state["turn"], "status": "inProgress", "items": [], "error": None}}})
            run(scenario.get("turn", []))
        else:
            send({"id": wire_id, "error": {"code": -32601, "message": f"unknown method {method}"}})


if __name__ == "__main__":
    sys.exit(main())
