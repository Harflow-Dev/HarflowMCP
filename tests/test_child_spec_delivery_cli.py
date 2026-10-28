"""No-paid evidence that a plan-mode child can read the immutable fallback without Harflow tools."""
import json
import os
import re
import hashlib
import shutil
import subprocess
import pytest
from claude_cli_support import isolate
from fake_anthropic_api import serve
from harflow_mcp.workflow_invocation import inherited_context
from harflow_mcp.workflow_prompt_delivery import bounded_inherited_context


@pytest.mark.cli_integration
@pytest.mark.skipif(not os.environ.get('HARFLOW_CLI_INTEGRATION'), reason='opt-in no-paid real CLI')
def test_plan_mode_can_read_complete_inherited_snapshot_without_harflow_tools(tmp_path, monkeypatch):
    executable = shutil.which('claude')
    if not executable:
        pytest.skip('Claude not installed')
    tmp_path = tmp_path.resolve()
    repo = tmp_path / 'repo'; repo.mkdir()
    parent = {'workflow_run_id': 'parent', 'technical_plan': 'Approved requirement ' * 1000, 'technical_plan_execution_id': 'approved-revision'}
    run = {'workflow_run_id': 'child-readability', 'inherited_context': inherited_context(parent, {'id': 'invoke'}, {'inputs': []})}
    manifest = bounded_inherited_context(run, tmp_path)
    path = manifest['retrieval']['readable_chunks_path']
    line_count = manifest['retrieval']['chunk_lines']
    results = []
    def decide(body):
        found = [block for message in body.get('messages', []) for block in (message.get('content') if isinstance(message.get('content'), list) else []) if isinstance(block, dict) and block.get('type') == 'tool_result']
        results[:] = found
        offset = 1 + len(found) * 16
        if offset <= line_count:
            return [{'type': 'tool_use', 'id': 'read-spec-' + str(offset), 'name': 'Read', 'input': {'file_path': path, 'offset': offset, 'limit': 16}}], 'tool_use'
        return [{'type': 'text', 'text': 'complete'}], 'end_turn'
    with serve(decide) as (url, requests):
        isolate(monkeypatch, tmp_path, url)
        result = subprocess.run([executable, '-p', '--permission-mode', 'plan', '--output-format', 'stream-json', '--verbose', '--', 'Read the complete approved parent specification.'], cwd=repo, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert results and all(not block.get('is_error') for block in results), results
    chunks = []
    for block in results:
        content = block['content']
        text = content if isinstance(content, str) else ''.join(part.get('text', '') for part in content if isinstance(part, dict))
        chunks.extend(json.loads(line) for line in re.findall(r'(?m)^\s*\d+(?:→|\t)(.*)$', text) if line.strip())
    assert len(chunks) == line_count, repr(results[0]["content"])[:800]
    restored = ''.join(chunks)
    assert hashlib.sha256(restored.encode()).hexdigest() == manifest['retrieval']['file_sha256']
    assert json.loads(restored)['payload']['technical_plan'] == parent['technical_plan']
