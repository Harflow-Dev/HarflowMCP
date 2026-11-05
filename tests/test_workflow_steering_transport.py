"""Steering transport preserves exact requests and typed refusals across ctl/MCP."""
import json
from types import SimpleNamespace

import pytest

from harflow_mcp import ctl, server, state_ownership
from harflow_mcp.state_layout import resolve_layout


@pytest.fixture(autouse=True)
def owned_state(tmp_path, monkeypatch):
    root = tmp_path / "state"
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(root))
    monkeypatch.setattr(server, "_reg", lambda: SimpleNamespace(log_dir=root / "tasks"))
    with state_ownership.state_session(resolve_layout(root=root)):
        pass


def test_cli_preserves_exact_request_text_and_shared_service(monkeypatch, tmp_path, capsys):
    from harflow_mcp import workflow_steering as steering
    text = "  discuss this\r\nquoted decision_id: dec1 🛠\n  "
    request = {"action": "open", "text": text}
    path = tmp_path / "request.json"
    path.write_bytes(json.dumps(request, ensure_ascii=False).encode())
    observed = []

    async def call(root_id, payload, **kwargs):
        observed.append((root_id, payload, kwargs))
        return {"accepted": True, "state": "queued_boundary"}

    monkeypatch.setattr(steering, "steer_workflow", call)
    assert ctl.main(["workflow-steer", "root1", "--request-file", str(path), "--monitor", "--json"]) == 0
    assert observed == [("root1", request, {"root": tmp_path / "state", "monitor": True})]
    assert json.loads(capsys.readouterr().out)["result"]["state"] == "queued_boundary"


@pytest.mark.parametrize("raw", [b'{"action":"open","action":"reply"}', b'{"value":NaN}', b'[]', b'\xff', b' ' * (128 * 1024 + 1)])
def test_bad_or_oversize_request_never_enters_service(raw, monkeypatch, tmp_path, capsys):
    from harflow_mcp import workflow_steering as steering
    path = tmp_path / "bad.json"
    path.write_bytes(raw)

    async def unexpected(*args, **kwargs):
        pytest.fail("Invalid request must not enter the mutation service")

    monkeypatch.setattr(steering, "steer_workflow", unexpected)
    assert ctl.main(["workflow-steer", "root1", "--request-file", str(path), "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "steering_invalid_request"


async def test_mcp_and_ctl_preserve_service_refusal_code(monkeypatch, tmp_path, capsys):
    from mcp import MCPError
    from harflow_mcp import workflow_steering as steering

    def refusal(*args, **kwargs):
        raise steering.SteeringError("steering_not_owner", "Original caller changed")

    monkeypatch.setattr(steering, "get_workflow_steering", refusal)
    with pytest.raises(MCPError) as error:
        await server.get_workflow_steering("root1")
    assert error.value.error.data["code"] == "steering_not_owner"
    # ctl owns its own event loop; invoke it outside this test's async loop below.


def test_ctl_preserves_authority_refusal(monkeypatch, capsys):
    from harflow_mcp import workflow_steering as steering

    def refusal(*args, **kwargs):
        raise steering.SteeringError("steering_not_owner", "Original caller changed")

    monkeypatch.setattr(steering, "get_workflow_steering", refusal)
    assert ctl.main(["workflow-steering-status", "root1", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "steering_not_owner"


async def test_mcp_cannot_assert_monitor_authority(monkeypatch):
    from harflow_mcp import workflow_steering as steering
    observed = []

    async def call(root_id, request, **kwargs):
        observed.append(kwargs)
        return {"accepted": True}

    monkeypatch.setattr(steering, "steer_workflow", call)
    await server.steer_workflow("root1", {"action": "take_owner"})
    assert "monitor" not in observed[0]


def test_history_cli_has_bounded_paging_arguments(monkeypatch, capsys):
    from harflow_mcp import workflow_steering as steering
    observed = []

    def call(root_id, **kwargs):
        observed.append((root_id, kwargs))
        return {"items": [], "has_more": False}

    monkeypatch.setattr(steering, "get_workflow_steering_history", call)
    assert ctl.main(["workflow-steering-history", "root1", "--cursor", "opaque", "--limit", "5", "--json"]) == 0
    assert observed[0][1]["cursor"] == "opaque"
    assert observed[0][1]["limit"] == 5
    assert json.loads(capsys.readouterr().out)["result"]["has_more"] is False
