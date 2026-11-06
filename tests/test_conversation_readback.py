"""Native captured display cannot recreate physical settlement or sticky errors."""
import uuid

import pytest

from harflow_mcp import state_ownership, store
from harflow_mcp.state_layout import resolve_layout


@pytest.mark.parametrize('observed', [False, True])
@pytest.mark.parametrize('sealed', [False, True])
def test_owner_recorded_native_display_preserves_outcome_without_legacy_replay(tmp_path, observed, sealed):
    layout = resolve_layout(root=tmp_path / 'state')
    task_id = str(uuid.uuid4())
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir()
        record = store.TaskRecord(task_id=task_id, backend='claude', session_id='native-a',
            repo_path=str(tmp_path), freedom='read_only', markers=[], started_at='2026-11-06T00:00:00+00:00',
            status='completed' if observed else 'failed', exit_code=0 if observed else None,
            output_drained=observed, terminal_summary='last captured response',
            native_output=True,
            conversation=None if sealed else dict(contract='conversation_delivery_v1', conversation_id=str(uuid.uuid4()),
                transport='claude-stream-json', profile_fingerprint='0' * 64))
        store.write(layout.tasks_directory, record)
        store.log_path(layout.tasks_directory, task_id).write_text(
            '{"type":"result","is_error":true,"subtype":"error_during_execution"}\n'
            '{"type":"result","is_error":false,"subtype":"success","result":"raw final"}\n')
        status, _, state, _ = store.resolve_status(layout.tasks_directory, record)
        assert status == ('completed' if observed else 'failed')
        assert state.summary == 'last captured response'
        assert state.is_error is (not observed)
        assert state.terminal is None  # The display readback invents no native/OS terminal proof.
