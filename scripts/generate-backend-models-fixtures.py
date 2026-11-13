#!/usr/bin/env python3
"""`backend-models.v1` ctl specimens: the real command and serializer over fake harness output.

`ctl.main` runs with an injected clock, a fake subprocess runner, a temporary environment and temporary harness
and state locations; no real harness is run and no state root exists. The recorded harness outputs under
`tests/data/backend_models` are the inputs. Older minimal version-info specimens keep their capability lists.
Run it after the steering, approval, handshake and pending-launch generators.
"""
import argparse
import contextlib
import hashlib
import io
import json
import os
import stat
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from harflow_mcp import bounded_process, ctl
from harflow_mcp.wire_contracts import CAPABILITIES, CTL_FORMAT, PRODUCT_VERSION

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / 'contracts/v1'
DATA = ROOT / 'tests/data/backend_models'
CAPABILITY = 'backend-models.v1'
NEGATIVE = 'invalid-s05-missing-backend-models.json'
CLOCK = datetime(2026, 11, 13, 12, 0, 0, tzinfo=timezone.utc)
CURSOR_VERSION = b'2026.11.11-b912975\n'
PROVENANCE = ('Actual ctl.main and envelope serializer over the production listing seam, with an injected clock, fake '
              'subprocess runner, temporary environment and no state root. Inputs are recorded harness outputs '
              '(measured 2026-11-13) or the stated synthetic change; no real harness runs. Synthetic evidence, not '
              'runtime certification.')


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False) + '\n').encode()


def many_models(count):
    text = ''
    for index in range(count):
        header = f'bulk/model-{index:04d}'
        text += header + '\n' + json.dumps({'id': f'model-{index:04d}', 'providerID': 'bulk', 'name': f'Bulk model {index}'}, indent=2) + '\n'
    return text.encode()


def done(stdout=b'', stderr=b'', returncode=0, cause=None):
    return bounded_process.Completed(None if cause else returncode, stdout, stderr, cause)


def runner_for(outputs):
    def run(argv, **kwargs):
        name, rest = os.path.basename(argv[0]), list(argv[1:])
        if rest == ['--version']:
            return done(CURSOR_VERSION)
        return outputs[name]
    return run


SCENARIOS = {
    'available-claude': dict(backend='claude', binaries=['claude']),
    'available-codex': dict(backend='codex', binaries=['codex'], codex=True),
    'available-opencode': dict(backend='opencode', binaries=['opencode'],
                               outputs={'opencode': done((DATA / 'opencode-models-verbose.txt').read_bytes())}),
    'available-antigravity': dict(backend='antigravity', binaries=['agy'],
                                  outputs={'agy': done((DATA / 'agy-models.out').read_bytes(), b'Fetching available models...\n')}),
    'available-cursor': dict(backend='cursor', binaries=['cursor-agent'],
                             outputs={'cursor-agent': done((DATA / 'cursor-models.out').read_bytes())}),
    'available-vibe': dict(backend='vibe', binaries=['vibe'], vibe=True),
    'not-installed': dict(backend='claude', binaries=[]),
    'unavailable-timeout': dict(backend='opencode', binaries=['opencode'], outputs={'opencode': done(cause='timeout')}),
    'unavailable-exit-nonzero': dict(backend='antigravity', binaries=['agy'], outputs={'agy': done(b'', b'boom\n', 3)}),
    'unavailable-auth-required': dict(backend='cursor', binaries=['cursor-agent'],
                                      outputs={'cursor-agent': done(b'', (DATA / 'cursor-models-loggedout.err').read_bytes(), 1)}),
    'unavailable-unparseable': dict(backend='antigravity', binaries=['agy'], outputs={'agy': done(b'no tab separators here\n')}),
    'unavailable-source-missing': dict(backend='codex', binaries=['codex']),
    'truncated': dict(backend='opencode', binaries=['opencode'], outputs={'opencode': done(many_models(401))},
                      note='Synthetic: 401 generated opencode models, so the 400-row cap applies.'),
    'agent-refused': dict(backend='codex', binaries=['codex'], codex=True, environment={'HARFLOW_TASK_ID': 'fixture-task'}, code=1),
    'usage-unknown-backend': dict(argv=['backend-models', '--backend', 'unknown-backend', '--json'], code=2),
}


def documents(base):
    docs = {}
    for scenario, spec in SCENARIOS.items():
        root = base / scenario
        bin_dir, home, codex_home, vibe_home = (root / name for name in ('bin', 'home', 'codex', 'vibe'))
        for directory in (bin_dir, home, codex_home, vibe_home):
            directory.mkdir(parents=True)
        for binary in spec.get('binaries', []):
            (bin_dir / binary).write_text('#!/bin/sh\nexit 0\n')
            (bin_dir / binary).chmod(0o755 | stat.S_IXUSR)
        if spec.get('codex'):
            (codex_home / 'models_cache.json').write_bytes((DATA / 'codex-models-cache.json').read_bytes())
        if spec.get('vibe'):
            (vibe_home / 'config.toml').write_bytes((DATA / 'vibe-config.toml').read_bytes())
        environment = {'PATH': str(bin_dir), 'HOME': str(home), 'CODEX_HOME': str(codex_home), 'VIBE_HOME': str(vibe_home),
                       'HARFLOW_STATE_ROOT': str(root / 'state'), **spec.get('environment', {})}
        argv = spec.get('argv') or ['backend-models', '--backend', spec['backend'], '--json']
        output = io.StringIO()
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, environment, clear=True))
            stack.enter_context(patch.object(ctl, '_model_listing_clock', return_value=CLOCK))
            stack.enter_context(patch.object(bounded_process, 'run', runner_for(spec.get('outputs', {}))))
            stack.enter_context(contextlib.redirect_stdout(output))
            stack.enter_context(contextlib.redirect_stderr(io.StringIO()))
            try:
                code = ctl.main(argv)
            except SystemExit as exc:
                code = exc.code
        assert code == spec.get('code', 0), (scenario, code)
        assert not (root / 'state').exists(), 'the command must not create a state root'
        text = output.getvalue()
        assert len(text.strip().splitlines()) == 1, scenario
        docs['ctl-backend-models-' + scenario + '.json'] = (json.loads(text), code, spec.get('note', ''))
    return docs


def generate():
    with tempfile.TemporaryDirectory(prefix='harflow-backend-models-fixtures-') as temporary:
        docs = documents(Path(temporary))
    manifest = json.loads((BUNDLE / 'manifest.json').read_bytes())
    output = {}
    for name, (doc, code, note) in sorted(docs.items()):
        output[BUNDLE / name] = canonical(doc)
        manifest['files'][name] = hashlib.sha256(output[BUNDLE / name]).hexdigest()
        manifest['cases'][name] = dict(encoding='json', expected_product_version=PRODUCT_VERSION, format=CTL_FORMAT,
                                       required_capabilities=[CAPABILITY] if 'result' in doc else [], valid=True,
                                       exit_code=code, provenance=(note + ' ' + PROVENANCE).strip())
    assert CAPABILITY in CAPABILITIES and NEGATIVE in manifest['cases'], 'run the handshake generator first'
    output[BUNDLE / 'manifest.json'] = (json.dumps(manifest, indent=2, ensure_ascii=False) + '\n').encode()
    output[BUNDLE / 'manifest.sha256'] = (hashlib.sha256(output[BUNDLE / 'manifest.json']).hexdigest() + '  manifest.json\n').encode()
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    output = generate()
    if args.check:
        assert output == generate(), 'Two fresh generations differ'
        different = [str(path.relative_to(ROOT)) for path, raw in output.items() if not path.exists() or path.read_bytes() != raw]
        if different:
            raise SystemExit('Fixtures differ: ' + ', '.join(different))
    else:
        for path, raw in output.items():
            path.write_bytes(raw)
