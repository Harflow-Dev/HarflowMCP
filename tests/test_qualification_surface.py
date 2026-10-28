"""Qualification discovery and explicit rerun use the same backend declarations."""
import json
from types import SimpleNamespace

from harflow_mcp import backends, ctl
from harflow_mcp.backends import qualification
from harflow_mcp.backends.base import UnsupportedCapability


def test_discovery_never_runs_feature_probes(monkeypatch):
    backend = SimpleNamespace(name='test-backend')
    observed = {'caller_approval_tools': {'source': 'auto-probed', 'ok': True}}
    monkeypatch.setattr(qualification, 'inspect', lambda value: observed if value is backend else None)
    monkeypatch.setattr(qualification, 'require', lambda *_a, **_k: (_ for _ in ()).throw(AssertionError('discovery launched a probe')))
    result = backends.qualification_metadata(backend)
    assert result['features'] == observed
    assert result['retry_command'] == 'harflow-ctl qualify test-backend'


def test_explicit_refresh_runs_bundle_once(monkeypatch, capsys):
    backend = SimpleNamespace(name='test-backend', qualification_features=lambda: {
        'caller_approval_tools': SimpleNamespace(revision='bundle1'),
        'caller_write_tools': SimpleNamespace(revision='bundle1')})
    monkeypatch.setattr(backends, 'get', lambda _: backend)
    calls = []
    def require(_backend, feature, refresh=False):
        calls.append((feature, refresh))
        return SimpleNamespace(as_dict=lambda: {'ok': True, 'source': 'auto-probed'})
    monkeypatch.setattr(qualification, 'require', require)
    assert ctl._cmd_qualify(SimpleNamespace(backend=backend.name, feature=None, json=True)) == 0
    result = json.loads(capsys.readouterr().out)['result']
    assert result['ok'] is True
    assert calls == [('caller_approval_tools', True), ('caller_write_tools', False)]


def test_failed_refresh_returns_specific_failure(monkeypatch, capsys):
    backend = SimpleNamespace(name='test-backend', qualification_features=lambda: {'allowed_commands': SimpleNamespace(revision='1')})
    monkeypatch.setattr(backends, 'get', lambda _: backend)
    def require(*_a, **_k):
        raise UnsupportedCapability('approval channel bypassed')
    monkeypatch.setattr(qualification, 'require', require)
    monkeypatch.setattr(qualification, 'inspect', lambda _: {'allowed_commands': {'ok': False, 'source': 'failed', 'reason': 'approval channel bypassed'}})
    assert ctl._cmd_qualify(SimpleNamespace(backend=backend.name, feature=None, json=True)) == 1
    result = json.loads(capsys.readouterr().out)['result']
    assert result['features']['allowed_commands']['reason'] == 'approval channel bypassed'
