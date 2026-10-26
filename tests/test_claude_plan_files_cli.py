"""Opt-in, no-paid Claude plan-write boundary probe against a localhost fake API."""
import json
import os
import re
import shutil
import subprocess

import pytest
from claude_cli_support import isolate
from fake_anthropic_api import serve

pytestmark = [pytest.mark.cli_integration,
              pytest.mark.skipif(not os.environ.get('HARFLOW_CLI_INTEGRATION'), reason='opt-in CLI probe')]


@pytest.mark.parametrize('custom', [False, True])
def test_claude_plan_file_write_exception(tmp_path, monkeypatch, custom):
    executable = shutil.which('claude')
    if not executable:
        pytest.skip('Claude not installed')
    version = subprocess.run([executable, '--version'], capture_output=True, text=True, check=True).stdout
    if not version.startswith('2.1.310 '):
        pytest.skip('Plan file exception measured on Claude 2.1.310')
    tmp_path = tmp_path.resolve()
    repo = tmp_path / 'repo'; repo.mkdir()
    if custom:
        (repo / '.claude').mkdir()
        (repo / '.claude/settings.json').write_text('{"plansDirectory":"custom-plans"}')
    plan_paths = []
    tool_results = []

    def decide(body):
        results = [block for message in body.get('messages', [])
                   for block in (message.get('content') if isinstance(message.get('content'), list) else [])
                   if isinstance(block, dict) and block.get('type') == 'tool_result']
        tool_results[:] = results
        if not results:
            matches = re.findall(r'/[^\s<>"\\]*plans/[^\s<>"\\]*\.md', json.dumps(body))
            assert matches, 'CLI must disclose its actual built-in plan path'
            plan_paths[:] = [matches[-1]]
        paths = [plan_paths[0], str(repo / 'ordinary.md'), str(tmp_path / 'outside.md')]
        index = len(results)
        if index < len(paths):
            return ([{'type': 'tool_use', 'id': f'plan_probe_{index}', 'name': 'Write',
                      'input': {'file_path': paths[index], 'content': 'local plan fixture'}}], 'tool_use')
        return ([{'type': 'text', 'text': 'finished'}], 'end_turn')

    with serve(decide) as (base_url, requests):
        isolate(monkeypatch, tmp_path, base_url)
        result = subprocess.run([executable, '-p', '--permission-mode', 'plan', '--output-format',
                                 'stream-json', '--verbose', '--', 'perform scripted writes'],
                                cwd=repo, capture_output=True, text=True, timeout=100)
    assert result.returncode == 0, result.stderr
    assert requests
    from pathlib import Path
    plan = Path(plan_paths[0])
    assert plan.parent == (repo / 'custom-plans' if custom else tmp_path / 'config/plans')
    assert plan.read_text() == 'local plan fixture'
    assert not tool_results[0].get('is_error')
    assert all(block.get('is_error') for block in tool_results[1:])
    assert not (repo / 'ordinary.md').exists()
    assert not (tmp_path / 'outside.md').exists()
