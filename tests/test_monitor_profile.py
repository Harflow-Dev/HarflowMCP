"""The app profiler must reject unsafe destinations before launching anything."""
import importlib.util
import json
from pathlib import Path

import pytest


@pytest.fixture
def profiler(monkeypatch):
    path = Path(__file__).resolve().parents[1] / 'scripts/profile-monitor.py'
    spec = importlib.util.spec_from_file_location('monitor_profiler', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module.platform, 'system', lambda: 'Darwin')
    return module


def arguments(monkeypatch, tmp_path, *, marker=True):
    app = tmp_path / 'Monitor.app'
    (app / 'Contents').mkdir(parents=True)
    (app / 'Contents/Info.plist').write_bytes(b'unmodified-source')
    home = tmp_path / 'home'
    (home / '.polybridge').mkdir(parents=True)
    wrappers = home / '.local/bin'
    wrappers.mkdir(parents=True)
    for tool in ('polybridge-ctl', 'polybridge-setup'):
        script = wrappers / tool
        script.write_text('#!/bin/sh\nexit 0\n')
        script.chmod(0o700)
    if marker:
        (home / 'monitor-benchmark-fixture.json').write_text(json.dumps({'version': 1}))
    output = tmp_path / 'output'
    monkeypatch.setattr('sys.argv', ['profile-monitor.py', '--app', str(app),
                                    '--home', str(home), '--output', str(output)])
    return app, home, output


def test_refuses_production_like_home_without_fixture_marker(profiler, monkeypatch, tmp_path):
    app, _, output = arguments(monkeypatch, tmp_path, marker=False)
    with pytest.raises(SystemExit) as error:
        profiler.main()
    assert error.value.code == 2
    assert not output.exists()
    assert (app / 'Contents/Info.plist').read_bytes() == b'unmodified-source'


def test_refuses_nonempty_output(profiler, monkeypatch, tmp_path):
    _, _, output = arguments(monkeypatch, tmp_path)
    output.mkdir()
    existing = output / 'previous-results.json'
    existing.write_text('keep me')
    with pytest.raises(SystemExit) as error:
        profiler.main()
    assert error.value.code == 2
    assert existing.read_text() == 'keep me'


def test_refuses_unknown_fixture_version(profiler, monkeypatch, tmp_path):
    _, home, output = arguments(monkeypatch, tmp_path)
    (home / 'monitor-benchmark-fixture.json').write_text(json.dumps({'version': 2}))
    with pytest.raises(SystemExit) as error:
        profiler.main()
    assert error.value.code == 2
    assert not output.exists()
