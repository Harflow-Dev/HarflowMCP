from types import SimpleNamespace

import pytest

from polybridge import backends, workflow_inspection as inspection


def caller():
    return SimpleNamespace(record=SimpleNamespace(freedom='read_only', network=False, backend='codex', repo_path='/repo', enforcement=backends.get('codex').enforcement('read_only', False).as_dict()))


def graph(config):
    return {'orchestrator': {'backend': 'codex'}, 'nodes': [{'type': 'agent', 'freedom': 'read_only', 'network': False, 'agent': config}]}


def test_unsupported_network_candidate_does_not_hide_safe_fallback(monkeypatch):
    monkeypatch.setattr(backends, 'is_installed', lambda backend: True)
    inspection.guard_saved_workflow_authority(caller(), graph({'backend': 'vibe', 'fallbacks': [{'backend': 'codex'}]}))


def test_every_runnable_candidate_must_preserve_authority(monkeypatch):
    monkeypatch.setattr(backends, 'is_installed', lambda backend: True)
    weak = backends.get('codex').enforcement('read_only', False).as_dict()
    weak['os_enforced'] = False
    monkeypatch.setattr(type(backends.get('opencode')), 'enforcement', lambda *args: weak)
    # A runnable backend claiming a weaker sandbox cannot hide behind Codex.
    with pytest.raises(backends.NestedDispatchRefused):
        inspection.guard_saved_workflow_authority(caller(), graph({'backend': 'vibe', 'fallbacks': [{'backend': 'codex'}, {'backend': 'opencode'}]}))


def test_all_unsupported_candidates_can_be_saved_without_dispatch(monkeypatch):
    # Save validates the envelope; missing/unsupported dispatch remains runtime state.
    monkeypatch.setattr(backends, 'is_installed', lambda backend: False)
    inspection.guard_saved_workflow_authority(caller(), graph({'backend': 'vibe'}))


def test_missing_binary_does_not_bypass_candidate_authority(monkeypatch):
    weak = backends.get('codex').enforcement('read_only', False).as_dict()
    weak['os_enforced'] = False
    monkeypatch.setattr(type(backends.get('opencode')), 'enforcement', lambda *args: weak)
    monkeypatch.setattr(backends, 'is_installed', lambda backend: False)
    with pytest.raises(backends.NestedDispatchRefused):
        inspection.guard_saved_workflow_authority(caller(), graph({'backend': 'codex', 'fallbacks': [{'backend': 'opencode'}]}))


def test_enforcement_authority_refusal_is_never_skipped_as_unsupported(monkeypatch):
    source = caller()
    backend = backends.get('codex')
    def refuse(*args, **kwargs):
        raise backends.NestedDispatchRefused('refused', rule='test')
    monkeypatch.setattr(type(backend), 'enforcement', refuse)
    with pytest.raises(backends.NestedDispatchRefused, match='refused'):
        inspection.guard_saved_workflow_authority(source, graph({'backend': 'codex'}))


def test_unsupported_candidate_cannot_relax_saved_access_ceiling(monkeypatch):
    monkeypatch.setattr(backends, 'is_installed', lambda backend: True)
    value = graph({'backend': 'vibe', 'fallbacks': [{'backend': 'codex'}]})
    value['nodes'][0]['freedom'] = 'publish'
    with pytest.raises(ValueError, match="cannot exceed the caller's freedom"):
        inspection.guard_saved_workflow_authority(caller(), value)
