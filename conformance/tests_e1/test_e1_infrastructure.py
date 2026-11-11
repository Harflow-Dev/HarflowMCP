"""Self-checks for the E1 read scenarios: port fidelity and sensitivity to a deviating engine."""
import hashlib
import json
import os
from pathlib import Path
import sys

import pytest

from conformance.e1 import cases, harness, scenarios, world
from conformance.e1.port import OPERATIONS, ExternalAdapter, PythonLayoutAdapter
from conformance.oracles import ConformanceError, strict_json

REAL = [sys.executable, '-I', '-B', '-m', 'harflow_mcp.cli']
PROXY = Path(__file__).with_name('proxy_engine.py')


def run_selected(tmp_path, mode, names, port=None, deltas=()):
    command = [sys.executable, '-I', '-B', str(PROXY), mode, *REAL, '--']
    rows = scenarios.run_cases(command, {}, lambda: None, tmp_path / 'out', selected=names, port=port, deltas=deltas)
    return {row['name']: row for row in rows}


def applied(tmp_path, name):
    log = tmp_path / 'out' / 'cases' / name / 'proxy-applied.log'
    return log.read_text().splitlines() if log.exists() else []


def assert_deviation(tmp_path, mode, name, expected_fragment):
    """The passthrough run of the same case succeeds, the doubled run fails for the stated reason, and the
    double provably applied its mutation."""
    base = run_selected(tmp_path / 'pass', 'passthrough', [name])[name]
    assert base['ok'], base['failures']
    assert applied(tmp_path / 'pass', name) == []
    doubled = run_selected(tmp_path / 'dbl', mode, [name])[name]
    assert not doubled['ok']
    assert applied(tmp_path / 'dbl', name), 'the proxy never applied its mutation'
    assert any(expected_fragment in message for message in doubled['failures']), doubled['failures'][:5]


def test_port_exposes_every_named_operation():
    adapter = PythonLayoutAdapter()
    assert all(callable(getattr(adapter, op)) for op in OPERATIONS)
    assert set(OPERATIONS) >= {'seed_world', 'add_task', 'add_live_caller', 'damage_task', 'append_event', 'append_partial_line',
                               'truncate_log', 'remove_log', 'tamper_prompt', 'write_artifacts', 'authoritative_digest'}


def test_seeded_world_matches_the_documented_private_envelopes(tmp_path):
    adapter = PythonLayoutAdapter()
    adapter.seed_world(tmp_path / 'root', world.WORLD_READS)
    tasks = tmp_path / 'root' / 'tasks'
    marker = strict_json((tmp_path / 'root' / '.harflow-store.json').read_bytes())
    assert marker == {'format': 'harflow.store', 'layout_version': 1, 'product': 'harflow', 'product_version': '0.0.13', 'v': 1}
    for task_id in (world.A, world.B, world.C, world.D, world.E):
        document = strict_json((tasks / f'{task_id}.meta.json').read_bytes())
        assert (document['product'], document['format'], document['v'], document['task_id']) == ('harflow', 'harflow.task', 1, task_id)
    for task_id in (world.A, world.B):
        events = [strict_json(line) for line in (tasks / f'{task_id}.events.jsonl').read_text().splitlines()]
        assert [e['seq'] for e in events] == list(range(len(events)))
        assert all((e['product'], e['format'], e['v']) == ('harflow', 'harflow.event', 1) for e in events)
    digest = hashlib.sha256(world.LONG_PROMPT.encode()).hexdigest()
    assert (tasks / f'{world.D}.prompt.{digest}.txt').read_text() == world.LONG_PROMPT
    assert len(world.LONG_PROMPT) > 2000 and len(world.LONG_PROMPT.encode()) > len(world.LONG_PROMPT)
    assert oct(os.stat(tasks / f'{world.A}.meta.json').st_mode & 0o777) == '0o600'


def test_authoritative_digest_sees_changes_and_ignores_derived_caches(tmp_path):
    adapter = PythonLayoutAdapter()
    empty = tmp_path / 'empty'
    adapter.initialize_empty(empty)
    adapter.seed_world(tmp_path / 'root', world.WORLD_READS)
    base = adapter.authoritative_digest(tmp_path / 'root')
    assert len(base) == 64 and base != adapter.authoritative_digest(empty)
    (tmp_path / 'root' / 'tasks' / '.listing.v11.sqlite3').write_bytes(b'cache')
    (tmp_path / 'root' / '.statistics.v1.sqlite3').write_bytes(b'cache')
    assert adapter.authoritative_digest(tmp_path / 'root') == base
    adapter.append_partial_line(tmp_path / 'root', world.A)
    assert adapter.authoritative_digest(tmp_path / 'root') != base


def test_seed_refuses_to_overwrite(tmp_path):
    adapter = PythonLayoutAdapter()
    adapter.seed_world(tmp_path / 'root', world.WORLD_READS)
    with pytest.raises(FileExistsError):
        adapter.add_task(tmp_path / 'root', world.WORLD_READS['tasks'][0])


def test_external_adapter_requires_its_pinned_digest(tmp_path):
    tool = tmp_path / 'tool'
    tool.write_text('#!/bin/sh\nexit 0\n')
    tool.chmod(0o755)
    with pytest.raises(ConformanceError):
        ExternalAdapter(tool, '0' * 64)
    adapter = ExternalAdapter(tool, hashlib.sha256(tool.read_bytes()).hexdigest())
    assert all(callable(getattr(adapter, op)) for op in OPERATIONS)


def test_every_case_is_registered_with_capabilities_and_a_default_tag():
    assert len(cases.CASES) == 56
    for name, (capabilities, function, tag) in cases.CASES.items():
        assert name.count('.') == 1 and callable(function) and 'ctl.v1' in capabilities and tag in ('baseline', 'contract')


def test_cases_never_touch_the_state_root_directly():
    modules = [Path(cases.__file__), *sorted(Path(cases.__file__).parent.glob('cases_*.py'))]
    source = ''.join(path.read_text() for path in modules)
    for forbidden in ('ctx.tasks', 'ctx.root /', 'seed.', 'shutil', 'write_text', 'write_bytes', '.meta.json', '.events.jsonl', '.artifacts.json',
                      '.launch.json', '.continuation-stopped.json'):
        assert forbidden not in source, forbidden


def test_scenarios_reject_an_engine_that_answers_nothing_useful(tmp_path):
    results = run_selected(tmp_path, 'empty', ['list.populated', 'status.exact_and_prefix', 'task-prompt.errors'])
    assert all(not row['ok'] and row['failures'] for row in results.values())
    assert applied(tmp_path, 'list.populated')


def test_passthrough_succeeds_for_the_cases_used_below(tmp_path):
    names = ['status.exact_and_prefix', 'list.populated', 'task-events.kind_filters']
    assert all(row['ok'] for row in run_selected(tmp_path, 'passthrough', names).values())


def test_dropped_resolved_from_is_detected(tmp_path):
    assert_deviation(tmp_path, 'drop-resolved-from', 'status.exact_and_prefix', 'prefix echo')


def test_wrong_status_is_detected(tmp_path):
    assert_deviation(tmp_path, 'wrong-status', 'status.exact_and_prefix', 'status.status')


def test_dropped_event_is_detected(tmp_path):
    assert_deviation(tmp_path, 'mutate-events', 'task-events.kind_filters', '--all-kinds adds assistant_delta')


def test_missing_authority_refusal_is_detected(tmp_path):
    assert_deviation(tmp_path, 'no-authority', 'scope.agent_caller_by_ancestry', 'error.code')


def test_ignored_environment_markers_are_detected(tmp_path):
    assert_deviation(tmp_path, 'strip-markers', 'scope.environment_markers', 'error.code')


def test_open_failure_on_an_undecidable_caller_is_detected(tmp_path):
    assert_deviation(tmp_path, 'open-undecidable', 'scope.incompatible_metadata_fails_closed', 'error.code')


def test_unbound_cursors_are_detected(tmp_path):
    assert_deviation(tmp_path, 'unbound-cursor', 'cursors.binding', 'error.code')


def test_a_read_that_rewrites_authoritative_state_is_a_failure(tmp_path):
    assert_deviation(tmp_path, 'mutate-state', 'list.populated', 'authoritative state unchanged')


def test_nonterminating_paging_fails_instead_of_hanging(tmp_path):
    for name, fragment in (('task-events.paging_roundtrip', 'within'), ('interaction-events.feed_and_paging', '')):
        assert_deviation(tmp_path / name, 'loop', name, fragment)


def test_a_case_that_raises_is_a_failure(tmp_path, monkeypatch):
    def broken(ctx):
        raise RuntimeError('boom')
    monkeypatch.setitem(cases.CASES, 'x.boom', (('ctl.v1',), broken, 'baseline'))
    result = run_selected(tmp_path, 'passthrough', ['x.boom'])
    assert not result['x.boom']['ok'] and 'boom' in result['x.boom']['failures'][0]


class VacuousDigest(PythonLayoutAdapter):
    def authoritative_digest(self, root):
        return 'a' * 64


class EmptyDigest(PythonLayoutAdapter):
    def authoritative_digest(self, root):
        return ''


@pytest.mark.parametrize('adapter, fragment', [(VacuousDigest, 'distinguishes a seeded store'), (EmptyDigest, 'empty authoritative digest')])
def test_a_vacuous_digest_cannot_pass(tmp_path, adapter, fragment):
    result = run_selected(tmp_path, 'passthrough', ['list.populated'], port=adapter())['list.populated']
    assert not result['ok'] and any(fragment in message for message in result['failures']), result['failures']


def test_framework_identity_covers_the_e1_package_recursively():
    identity = scenarios.framework_identity()
    assert 'e1/cases.py' in identity and 'e1/port.py' in identity and 'oracles.py' in identity


REPO_ROOT = Path(__file__).resolve().parents[2]


def make_tool(tmp_path):
    """An external fixture tool speaking the JSON protocol, backed by the Python adapter."""
    path = tmp_path / 'adapter'
    path.write_text(f"#!{sys.executable}\nimport json, sys\nsys.path.insert(0, {str(REPO_ROOT)!r})\n"
                    "from conformance.e1.port import PythonLayoutAdapter\n"
                    "op = sys.argv[1]; root = sys.argv[sys.argv.index('--root') + 1]\n"
                    "arguments = json.loads(sys.stdin.read() or '{}')\n"
                    "print(json.dumps(getattr(PythonLayoutAdapter(), op)(root, **arguments)))\n")
    path.chmod(0o755)
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def test_external_adapter_round_trips_every_argument_name(tmp_path):
    path, digest = make_tool(tmp_path)
    external, direct = ExternalAdapter(path, digest), PythonLayoutAdapter()
    for adapter, root in ((external, tmp_path / 'ext'), (direct, tmp_path / 'dir')):
        adapter.seed_world(root, world.WORLD_READS)
        adapter.add_task(root, world.settled('77777777-7777-4777-8777-777777777777', prompt='p'))
        adapter.append_event(root, world.A, 8, 'assistant_text', '2026-01-10T10:06:00+00:00', {'text': 'x'})
        adapter.append_malformed_line(root, world.A)
        adapter.truncate_log(root, world.A, 3)
        adapter.damage_task(root, world.BROKEN)
        adapter.write_artifacts(root, world.A, [world.artifact('11111111-2222-4333-8444-555555555555', 'a.txt')], True)
    assert external.root_state(tmp_path / 'ext') == 'initialized' and external.root_state(tmp_path / 'none') == 'missing'
    assert external.authoritative_digest(tmp_path / 'ext') == direct.authoritative_digest(tmp_path / 'dir')
    assert external.add_live_caller(tmp_path / 'ext', '12121212-1212-4212-8212-121212121212', os.getpid(), 'alive', 'running', None) == {'supported': True}
    assert external.add_live_caller(tmp_path / 'ext', '34343434-3434-4434-8434-343434343434', os.getpid(), 'nonsense', 'running', None) == {'supported': False}
    assert external.identity()['sha256'] == digest
    for adapter, root in ((external, tmp_path / 'ext'), (direct, tmp_path / 'dir')):
        adapter.stop_chain(root, {'chain_root': world.A, 'mode': 'marker', 'attribution': world.marker_attribution('valid', world.C)})
        adapter.add_launch(root, world.launch(world.launch_id('1'), 'reserved'))
        adapter.add_invalid_launch_name(root, 'bad name')
    # Receipts record their root path and directory inode, so compare names and the marker bytes, not a digest.
    listing = lambda root: sorted(path.name for path in (root / 'tasks').iterdir() if path.name.endswith(('.launch.json', '.continuation-stopped.json')))
    assert listing(tmp_path / 'ext') == listing(tmp_path / 'dir')
    marker = f'{world.A}.continuation-stopped.json'
    assert (tmp_path / 'ext' / 'tasks' / marker).read_bytes() == (tmp_path / 'dir' / 'tasks' / marker).read_bytes()


def test_external_adapter_digest_is_rechecked_before_each_call(tmp_path):
    path, digest = make_tool(tmp_path)
    adapter = ExternalAdapter(path, digest)
    adapter.root_state(tmp_path / 'x')
    path.write_text(path.read_text() + '# changed\n')
    with pytest.raises(ConformanceError):
        adapter.root_state(tmp_path / 'x')


def test_root_state_requires_the_store_lock(tmp_path):
    adapter = PythonLayoutAdapter()
    adapter.initialize_empty(tmp_path / 'root')
    assert adapter.root_state(tmp_path / 'root') == 'initialized'
    (tmp_path / 'root' / '.harflow-store.lock').unlink()
    assert adapter.root_state(tmp_path / 'root') == 'other'


def test_task_specs_are_complete_and_the_adapter_has_no_defaults(tmp_path):
    adapter = PythonLayoutAdapter()
    adapter.initialize_empty(tmp_path / 'root')
    spec = world.settled('88888888-8888-4888-8888-888888888888')
    assert {'backend', 'repo_path', 'freedom', 'session_id'} <= set(spec)
    del spec['freedom']
    with pytest.raises(KeyError):
        adapter.add_task(tmp_path / 'root', spec)


def test_combined_tags_are_not_relabelled(tmp_path):
    ctx = harness.Ctx('x.y', tmp_path / 'c', ['unused'], (), default_tag='baseline')
    ctx.check(True, 'one [contract, pending U10]')
    ctx.check(True, 'two [baseline, U4]')
    ctx.check(True, 'three [proposed, interim-6.1]')
    ctx.check(True, 'four')
    assert [row['message'] for row in ctx.checks] == ['one [contract, pending U10]', 'two [baseline, U4]',
                                                      'three [proposed, interim-6.1]', 'four [baseline]']


def test_acceptance_rules_for_skips_and_exclusions():
    ran = lambda *skips: [{'name': n, 'ok': True, 'skips': list(skips)} for n in cases.CASES]
    legacy = {'kind': 'legacy', 'name': 'legacy_no_markers/running', 'message': 'm'}
    unverified = {'kind': 'unverified', 'name': 'recovery-activity.capture_loss', 'message': 'm'}
    everything = lambda results, selected=None, exclusions=(): scenarios.assess(results, selected, exclusions)
    assert everything(ran(legacy))['complete'] and everything(ran(legacy))['ok']
    blocked = everything(ran(unverified))
    assert blocked['ok'] and not blocked['complete'] and blocked['unverified_unexcluded'] == ['recovery-activity.capture_loss']
    excluded = everything(ran(unverified), exclusions=['recovery-activity.capture_loss'])
    assert excluded['complete'] and excluded['exclusions'] == ['recovery-activity.capture_loss']
    assert not everything(ran(), selected=['list.populated'])['complete']
    failing = ran()
    failing[0]['ok'] = False
    assert not everything(failing)['ok']
    assert not everything([])['ok']


class NoReused(PythonLayoutAdapter):
    def add_live_caller(self, root, task_id, pid, identity='alive', status='running', marker=None):
        return {'supported': False} if identity == 'reused' else super().add_live_caller(root, task_id, pid, identity, status, marker)


class NoLegacy(PythonLayoutAdapter):
    def add_live_caller(self, root, task_id, pid, identity='alive', status='running', marker=None):
        return {'supported': False} if identity.startswith('legacy_') else super().add_live_caller(root, task_id, pid, identity, status, marker)


def test_an_unsupported_required_identity_fails_but_legacy_ones_only_skip(tmp_path):
    required = run_selected(tmp_path / 'a', 'passthrough', ['scope.identity_verdicts'], port=NoReused())['scope.identity_verdicts']
    assert not required['ok'] and any('required identity state reused/running' in m for m in required['failures'])
    legacy = run_selected(tmp_path / 'b', 'passthrough', ['scope.identity_verdicts'], port=NoLegacy())['scope.identity_verdicts']
    assert legacy['ok'] and legacy['skips'] and all(s['kind'] == 'legacy' for s in legacy['skips'])


def test_delta_profiles_have_live_alternate_expectations(tmp_path):
    """Python stays on the baseline, so each proposed profile must fail against it."""
    u10 = run_selected(tmp_path / 'u10', 'passthrough', ['list.missing_root_initializes'], deltas=['U10'])['list.missing_root_initializes']
    assert not u10['ok'] and any('creates nothing' in m for m in u10['failures'])
    interim = run_selected(tmp_path / 'i', 'passthrough', ['scope.agent_caller_by_ancestry'], deltas=['interim-6.1'])['scope.agent_caller_by_ancestry']
    assert not interim['ok'] and any('interim-6.1' in m for m in interim['failures'])
    base = run_selected(tmp_path / 'b', 'passthrough', ['list.missing_root_initializes'])['list.missing_root_initializes']
    assert base['ok']


def test_prefix_paging_delta_fails_python_only_in_the_bug_assertion(tmp_path):
    name = 'pending-launch.prefix_nested_ids'
    baseline = run_selected(tmp_path / 'base', 'passthrough', [name])[name]
    assert baseline['ok'], baseline['failures']
    delta = run_selected(tmp_path / 'delta', 'passthrough', [name], deltas=['pending-prefix-paging'])[name]
    assert not delta['ok'] and len(delta['failures']) == 1 and 'pending-prefix-paging' in delta['failures'][0], delta['failures']


class SilentCaller(PythonLayoutAdapter):
    answer = None

    def add_live_caller(self, root, task_id, pid, identity='alive', status='running', marker=None):
        return self.answer


@pytest.mark.parametrize('answer', [None, {}, {'supported': 'yes'}, {'supported': 1}, 'ok'])
def test_an_adapter_that_does_not_answer_explicitly_cannot_pass_or_skip(tmp_path, answer):
    adapter = SilentCaller()
    adapter.answer = answer
    result = run_selected(tmp_path, 'passthrough', ['scope.identity_verdicts'], port=adapter)['scope.identity_verdicts']
    assert not result['ok'] and not result['skips']
    assert any('explicit boolean' in message for message in result['failures']), result['failures']


def test_artifact_items_are_complete_and_the_adapter_has_no_defaults(tmp_path):
    adapter = PythonLayoutAdapter()
    adapter.seed_world(tmp_path / 'root', world.WORLD_READS)
    item = world.artifact('11111111-2222-4333-8444-555555555555', 'a.txt')
    assert {'kind', 'note', 'registered_at', 'provenance_method', 'observation', 'target'} <= set(item)
    for missing in ('kind', 'note', 'registered_at', 'provenance_method', 'observation'):
        broken = {k: v for k, v in item.items() if k != missing}
        with pytest.raises(KeyError):
            adapter.write_artifacts(tmp_path / 'root', world.A, [broken], True)
    adapter.write_artifacts(tmp_path / 'root', world.A, [item], True)
    manifest = json.loads((tmp_path / 'root' / 'tasks' / f'{world.A}.artifacts.json').read_text())
    row = manifest['items'][0]
    assert (row['kind'], row['note'], row['registered_at'], row['registered_observation']) == \
        (world.ARTIFACT_KIND, world.ARTIFACT_NOTE, world.ARTIFACT_REGISTERED_AT, world.ABSENT_OBSERVATION)


def test_port_docstring_names_all_twenty_operations():
    from conformance.e1 import port
    assert len(port.OPERATIONS) == 20
    for op in port.OPERATIONS:
        assert f'{op}(' in port.__doc__, op


def test_stop_markers_match_the_python_writer_and_edge_shapes(tmp_path):
    adapter = PythonLayoutAdapter()
    adapter.seed_world(tmp_path / 'root', world.WORLD_READS)
    tasks = tmp_path / 'root' / 'tasks'
    adapter.stop_chain(tmp_path / 'root', {'chain_root': world.A, 'mode': 'marker', 'attribution': world.marker_attribution('valid', world.C)})
    assert json.loads((tasks / f'{world.A}.continuation-stopped.json').read_text()) == {'cancelled_task_id': world.C}
    adapter.stop_chain(tmp_path / 'root', {'chain_root': world.B, 'mode': 'marker', 'attribution': world.marker_attribution('oversize', world.C)})
    oversize = (tasks / f'{world.B}.continuation-stopped.json')
    assert oversize.stat().st_size > 4096 and json.loads(oversize.read_text())['cancelled_task_id'] == world.C
    adapter.stop_chain(tmp_path / 'root', {'chain_root': world.D, 'mode': 'marker', 'attribution': world.marker_attribution('malformed', None)})
    with pytest.raises(ValueError):
        json.loads((tasks / f'{world.D}.continuation-stopped.json').read_text())
    adapter.stop_chain(tmp_path / 'root', {'chain_root': world.E, 'mode': 'fault'})
    assert (tasks / f'{world.E}.continuation-stopped.json').is_dir()
    adapter.stop_chain(tmp_path / 'root', {'chain_root': world.E, 'mode': 'absent'})
    adapter.stop_chain(tmp_path / 'root', {'chain_root': world.A, 'mode': 'absent'})
    assert not (tasks / f'{world.E}.continuation-stopped.json').exists() and not (tasks / f'{world.A}.continuation-stopped.json').exists()
    with pytest.raises(FileExistsError):
        adapter.stop_chain(tmp_path / 'root', {'chain_root': world.B, 'mode': 'marker', 'attribution': world.marker_attribution('valid', world.C)})


def test_chain_root_follows_python_semantics(tmp_path):
    adapter = PythonLayoutAdapter()
    adapter.seed_world(tmp_path / 'root', world.WORLD_READS)
    tasks = tmp_path / 'root' / 'tasks'
    stored = lambda task_id: json.loads((tasks / f'{task_id}.meta.json').read_text()).get('chain_root_task_id', '<absent>')
    assert stored(world.C) == world.A     # continuation of the first task
    middle, last, raw, none = (f'12345678-0000-4000-8000-0000000000{n}' for n in ('a1', 'a2', 'a3', 'a4'))
    adapter.add_task(tmp_path / 'root', world.settled(middle, continuation_of=world.C))
    assert stored(middle) == world.A      # the predecessor's chain root, not the predecessor
    adapter.add_task(tmp_path / 'root', world.settled(last, continuation_of=world.B))
    assert stored(last) == world.B        # a predecessor without a stored root is the root itself
    adapter.add_task(tmp_path / 'root', world.settled(raw, continuation_of=world.A, chain_root='not valid'))
    assert stored(raw) == 'not valid'
    adapter.add_task(tmp_path / 'root', world.settled(none, continuation_of=world.A, chain_root=None))
    assert stored(none) == '<absent>'


def test_launch_receipts_are_qualified_and_replace_in_place(tmp_path):
    adapter = PythonLayoutAdapter()
    adapter.initialize_empty(tmp_path / 'root')
    tasks = tmp_path / 'root' / 'tasks'
    one, two = world.launch_id('1'), world.launch_id('2')
    adapter.add_launch(tmp_path / 'root', world.launch(one, 'waiting_for_resources', resources={'names': ['x']}))
    adapter.add_launch(tmp_path / 'root', world.launch(two, 'reserved', refusal={'check': 'format'}))
    adapter.add_invalid_launch_name(tmp_path / 'root', 'bad name')
    before = sorted(path.name for path in tasks.iterdir())
    document = json.loads((tasks / f'{one}.launch.json').read_text())
    assert (document['format'], document['state'], document['task_id'], document['root']) == ('harflow.task-launch', 'waiting_for_resources', one, str(tmp_path / 'root'))
    assert document['resources'] == {'names': ['x']} and len(document['generation']) == 32 and len(document['request_sha256']) == 64
    assert json.loads((tasks / f'{two}.launch.json').read_text())['format'] == 'harflow.other'
    adapter.add_launch(tmp_path / 'root', world.launch(one, 'granted', title='again'))
    assert sorted(path.name for path in tasks.iterdir()) == before
    assert json.loads((tasks / f'{one}.launch.json').read_text())['title'] == 'again'
    for check in ('oversize', 'resource_names', 'field_shape'):
        adapter.add_launch(tmp_path / 'root', world.launch(world.launch_id('3'), 'reserved', refusal={'check': check}))
    assert (tasks / f'{world.launch_id("3")}.launch.json').stat().st_size > 0
    with pytest.raises(ValueError):
        adapter.add_launch(tmp_path / 'root', world.launch(one, 'reserved', refusal={'check': 'unknown'}))


def test_new_cases_run_through_the_external_adapter_protocol(tmp_path):
    path, digest = make_tool(tmp_path)
    names = ['continuation-stop.invalid_root', 'pending-launch.cursor_lifecycle', 'pending-launch.empty_objects']
    results = run_selected(tmp_path / 'run', 'passthrough', names, port=ExternalAdapter(path, digest))
    assert all(row['ok'] for row in results.values()), {name: row['failures'][:2] for name, row in results.items()}


def _pending_cursor(value):
    import base64
    return base64.urlsafe_b64encode(json.dumps(value).encode()).decode()


@pytest.mark.parametrize('cursor', ['!!not base64!!', _pending_cursor([1, 2]), _pending_cursor({'v': 1}), _pending_cursor({'last': 5}),
                                    _pending_cursor({'last': 'other'})])
def test_uncraftable_pending_cursors_record_the_named_skip(tmp_path, cursor):
    from conformance.e1 import cases_pending
    ctx = harness.Ctx('x.y', tmp_path / 'c', ['unused'], ())
    assert cases_pending.reposition(ctx, cursor, 'one', 'zero') is None
    assert [skip['name'] for skip in ctx.skips] == ['pending-launch.cursor_position'] and ctx.skips[0]['kind'] == 'unverified'


def test_reposition_keeps_the_form_of_the_position(tmp_path):
    import base64
    from conformance.e1 import cases_pending
    ctx = harness.Ctx('x.y', tmp_path / 'c', ['unused'], ())
    for last in ('one', 'one.launch.json'):
        crafted = cases_pending.reposition(ctx, _pending_cursor({'last': last, 'v': 1}), 'one', 'zero')
        assert json.loads(base64.urlsafe_b64decode(crafted + '=' * (-len(crafted) % 4)))['last'] == last.replace('one', 'zero')
    assert not ctx.skips
