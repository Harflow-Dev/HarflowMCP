"""Workflow control text uses bounded argv and lossless UTF-8 files."""
import json

import pytest

from polybridge import ctl, server


@pytest.mark.parametrize('action,field', [('resume', 'instructions'), ('recover', 'reason')])
@pytest.mark.parametrize('monitor', [False, True])
def test_large_control_file_preserves_text_and_routing(tmp_path, monkeypatch, capsys, action, field, monitor):
    text = '  --option-shaped answer\r\n' + 'résolved 🛠\r\n' * 100_000 + '\n  '
    path = tmp_path / 'control.txt'
    path.write_bytes(text.encode('utf-8'))
    observed = {}

    async def call(selected, **kwargs):
        observed.update(action=selected, **kwargs)
        return {'status': 'running'}

    monkeypatch.setattr(server, '_workflow_call', call)
    argv = ['workflow-' + action, 'r1', '--' + field + '-file', str(path), '--additional-attempts', '2', '--json']
    if action == 'resume':
        argv += ['--decision-id', 'exact-decision']
    if monitor:
        argv += ['--monitor']
    assert max(map(len, argv)) < 1024
    assert ctl.main(argv) == 0
    assert observed['action'] == action
    assert observed['instructions'] == text
    assert observed['run_id'] == 'r1'
    assert observed['additional_attempts'] == 2
    if action == 'resume':
        assert observed['decision_id'] == 'exact-decision'
    assert observed.get('interaction_owner') == ('monitor' if monitor else None)
    assert json.loads(capsys.readouterr().out)['result']['status'] == 'running'


@pytest.mark.parametrize('action,field', [('resume', 'instructions'), ('recover', 'reason')])
def test_control_file_read_failure_never_dispatches(tmp_path, monkeypatch, capsys, action, field):
    async def unexpected(*args, **kwargs):
        pytest.fail('Unreadable control text must not dispatch')

    monkeypatch.setattr(server, '_workflow_call', unexpected)
    path = tmp_path / 'missing.txt'
    assert ctl.main(['workflow-' + action, 'r1', '--' + field + '-file', str(path), '--json']) == 1
    assert json.loads(capsys.readouterr().out)['error']['code'] == 'workflow_error'


@pytest.mark.parametrize('action,field', [('resume', 'instructions'), ('recover', 'reason')])
def test_control_file_and_inline_are_exclusive(tmp_path, action, field):
    with pytest.raises(SystemExit):
        ctl.main(['workflow-' + action, 'r1', '--' + field, 'inline', '--' + field + '-file', str(tmp_path / 'text')])
