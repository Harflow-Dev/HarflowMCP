"""E1 cases for `task-continuation-stop.v1` and the stored chain-root shapes the stop observer reads.

Expectations derive from `docs/contracts.md` ("Continuation-stop observations") and from the measurement of
Python 0.0.13 recorded for an invalid stored chain root (status, brief and batch report `unknown`; the
`task-list-page` related-header traversal is the one read that fails). Markers and chain roots are created only
through the fixture port.
"""
from . import cases as base
from .cases import CORE, case, error, ids, settled_page, subset, success
from .world import A, B, C, D, E, marker_attribution, settled

STOP = CORE + ('task-continuation-stop.v1',)
KEYS = {'state', 'chain_root_task_id', 'reason', 'cancelled_task_id', 'observed_at'}
BAD_ROOT = 'not a valid id!'
SECOND_MEMBER = '12345678-0000-4000-8000-0000000000bb'
THIRD_MEMBER = '12345678-0000-4000-8000-0000000000cc'


def observation(task, label, ctx):
    stop = task.get('continuation_stop')
    ctx.check(isinstance(stop, dict) and set(stop) == KEYS, f'{label}: continuation_stop has exactly the five contract fields [contract]')
    if isinstance(stop, dict):
        base.now_close(ctx, stop.get('observed_at'), f'{label}: observed_at')
    return stop or {}


def expect(ctx, stop, state, root, cancelled, label):
    reason = 'chain_cancelled' if state == 'stopped' else None
    subset(ctx, stop, {'state': state, 'chain_root_task_id': root, 'reason': reason, 'cancelled_task_id': cancelled}, label)


def three_ways(ctx, task_id):
    """The continuation_stop object from status, status --brief and a status-batch row."""
    status = success(ctx, ctx.ctl('status', task_id, '--json'))['task']
    brief = success(ctx, ctx.ctl('status', task_id, '--brief', '--json'))['task']
    rows = success(ctx, ctx.ctl('status-batch', '--task-ids', task_id, '--json'))['tasks']
    row = next((r for r in rows if r.get('task_id') == task_id), {})
    return (observation(status, f'status {task_id[:8]}', ctx), observation(brief, f'status --brief {task_id[:8]}', ctx),
            observation(row, f'status-batch {task_id[:8]}', ctx))


@case('continuation-stop.not_stopped_and_stopped', STOP)
def _(ctx):
    ctx.seed_reads()
    for task_id in (A, B, C):
        for stop in three_ways(ctx, task_id):
            expect(ctx, stop, 'not_stopped', A if task_id in (A, C) else B, None, f'{task_id[:8]} without a marker [contract]')
    ctx.fx.stop_chain({'chain_root': A, 'mode': 'marker', 'attribution': marker_attribution('valid', C)})
    ctx.rebase()
    for task_id in (A, C):
        for stop in three_ways(ctx, task_id):
            expect(ctx, stop, 'stopped', A, C, f'{task_id[:8]} observes its chain root marker with attribution [contract]')
    for stop in three_ways(ctx, B):
        expect(ctx, stop, 'not_stopped', B, None, 'a task outside the chain is not stopped [contract]')
    # Only the marker named by the chain root counts: a marker named after a real member does not stop its chain.
    ctx.fx.stop_chain({'chain_root': A, 'mode': 'absent'})
    ctx.fx.add_task(settled(SECOND_MEMBER, prompt='member', continuation_of=C))
    ctx.fx.stop_chain({'chain_root': C, 'mode': 'marker', 'attribution': marker_attribution('valid', C)})
    ctx.rebase()
    for task_id in (C, SECOND_MEMBER):
        for stop in three_ways(ctx, task_id):
            expect(ctx, stop, 'not_stopped', A, None, f'{task_id[:8]}: a marker named after a member does not stop the chain [contract]')
    ctx.fx.stop_chain({'chain_root': B, 'mode': 'marker', 'attribution': marker_attribution('valid', B)})
    ctx.rebase()
    for stop in three_ways(ctx, B):
        expect(ctx, stop, 'stopped', B, B, 'a task is stopped by the marker named after itself as chain root [contract]')
    # A continuation of a member carries the first task as chain root.
    ctx.fx.stop_chain({'chain_root': A, 'mode': 'marker', 'attribution': marker_attribution('valid', C)})
    ctx.rebase()
    for stop in three_ways(ctx, SECOND_MEMBER):
        expect(ctx, stop, 'stopped', A, C, 'a continuation of a member carries the first task as chain root [contract]')
    ctx.fx.stop_chain({'chain_root': A, 'mode': 'absent'})
    ctx.rebase()
    for stop in three_ways(ctx, SECOND_MEMBER):
        expect(ctx, stop, 'not_stopped', A, None, 'removing the marker restores not_stopped [contract]')


@case('continuation-stop.attribution_kinds', STOP)
def _(ctx):
    ctx.seed_reads()
    scenarios = (
        (A, 'valid', 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', A),
        (B, 'invalid', 'not a task id', None),
        (D, 'malformed', None, None),
        (E, 'oversize', C, None),
    )
    for root, kind, cancelled, expected in scenarios:
        ctx.fx.stop_chain({'chain_root': root, 'mode': 'marker', 'attribution': marker_attribution(kind, cancelled)})
    ctx.rebase()
    for root, kind, cancelled, expected in scenarios:
        for stop in three_ways(ctx, root):
            expect(ctx, stop, 'stopped', root, expected,
                   f'a {kind} marker still means stopped; attribution only when valid and readable [contract]')


@case('continuation-stop.unreadable_marker_is_unknown', STOP)
def _(ctx):
    ctx.seed_reads()
    ctx.fx.stop_chain({'chain_root': A, 'mode': 'fault'})
    ctx.rebase()
    for task_id in (A, C):
        for stop in three_ways(ctx, task_id):
            expect(ctx, stop, 'unknown', A, None, f'{task_id[:8]}: a marker that is not a safe regular file is unknown [contract]')
    for stop in three_ways(ctx, B):
        expect(ctx, stop, 'not_stopped', B, None, 'a fault in one chain does not touch another [contract]')


@case('continuation-stop.invalid_root', STOP)
def _(ctx):
    ctx.seed_reads()
    # Stored chain roots that are not task identities: the observer never falls back to another identity.
    ctx.fx.add_task(settled(SECOND_MEMBER, prompt='member', continuation_of=A, chain_root=BAD_ROOT))
    ctx.fx.add_task(settled(THIRD_MEMBER, prompt='member', continuation_of=A, chain_root='a' * 70))
    # An absent stored root falls back to the task's own identity, not to its predecessor's.
    absent = '12345678-0000-4000-8000-0000000000dd'
    ctx.fx.add_task(settled(absent, prompt='member', continuation_of=A, chain_root=None))
    ctx.fx.stop_chain({'chain_root': A, 'mode': 'marker', 'attribution': marker_attribution('valid', A)})
    ctx.rebase()
    for task_id in (SECOND_MEMBER, THIRD_MEMBER):
        for stop in three_ways(ctx, task_id):
            expect(ctx, stop, 'unknown', None, None, f'{task_id[-2:]}: an invalid stored chain root is unknown with a null root [contract]')
    for stop in three_ways(ctx, absent):
        expect(ctx, stop, 'not_stopped', absent, None, 'an absent stored chain root means the task is its own root [contract]')
    batch = success(ctx, ctx.ctl('status-batch', '--task-ids', f'{SECOND_MEMBER},{A}', '--json'))
    ctx.eq(batch['errors'], {}, 'a task with an invalid chain root does not fail the batch [baseline]')
    states = {row['task_id']: row['continuation_stop']['state'] for row in batch['tasks']}
    ctx.eq(states, {SECOND_MEMBER: 'unknown', A: 'stopped'}, 'the other rows of the batch are unaffected [baseline]')
    status = success(ctx, ctx.ctl('status', SECOND_MEMBER, '--json'))['task']
    ctx.eq(status.get('chain_root_task_id'), BAD_ROOT, 'status still echoes the stored value [baseline]')


@case('continuation-stop.invalid_root_consumers', STOP)
def _(ctx):
    """What the other reads do with a stored chain root that is not a task identity (Python 0.0.13, measured)."""
    ctx.seed_reads()
    ctx.fx.add_task(settled(SECOND_MEMBER, prompt='member', continuation_of=A, chain_root=BAD_ROOT,
                            started_at='2026-01-15T10:00:00+00:00', finished_at='2026-01-15T10:01:00+00:00'))
    ctx.fx.append_event(SECOND_MEMBER, 0, 'task_started', '2026-01-15T10:00:00+00:00', {'backend': 'claude'})
    ctx.fx.append_event(SECOND_MEMBER, 1, 'task_finished', '2026-01-15T10:01:00+00:00', {'status': 'completed', 'exit_code': 0})
    ctx.rebase()
    listing = success(ctx, ctx.ctl('list', '--json'))
    rows = {row['task_id']: row for row in listing['tasks']}
    ctx.eq(rows[SECOND_MEMBER].get('chain_root_task_id'), BAD_ROOT, 'list echoes the raw stored root [baseline]')
    ctx.eq((listing['history_incomplete'], listing['refused_tasks']), (False, []), 'list is complete and refuses nothing [baseline]')
    stats = base.stats_ready(ctx, 'statistics-page', '--since', '2026-01-01T00:00:00+00:00', '--before', '2026-02-01T00:00:00+00:00', '--json')
    got = {item.get('task_id'): item.get('chain_root_task_id') for item in success(ctx, stats)['items']}
    ctx.eq(got.get(SECOND_MEMBER, '<missing>'), BAD_ROOT, 'statistics-page echoes the raw stored root [baseline]')
    ctx.eq(success(ctx, stats).get('history_complete'), True, 'statistics-page history stays complete [baseline]')
    events = success(ctx, ctx.ctl('task-events', SECOND_MEMBER, '--json'))
    ctx.eq([e['seq'] for e in events.get('events', [])], [0, 1], 'task-events returns every event of the task [baseline]')
    feed = success(ctx, ctx.ctl('interaction-events', '--json'))
    ctx.check(SECOND_MEMBER in {e.get('task_id') for e in feed.get('events', [])}, 'the interaction feed includes the task [baseline]')
    # The related-header traversal behind task-list-page validates every linked id and fails the request.
    page = settled_page(ctx, 'task-list-page', '--json')
    error(ctx, page, 'invalid_params', label='task-list-page whose rows include the task')
    error(ctx, ctx.ctl('task-list-page', '--json', '--limit', '1'), 'invalid_params', label='task-list-page limit 1 (the task is the newest)')
    for args in (['--task-ids', SECOND_MEMBER], ['--predecessor-task-id', A]):
        error(ctx, ctx.ctl('task-list-page', '--json', *args), 'invalid_params', label='task-list-page ' + args[0])
    ok = success(ctx, ctx.ctl('task-list-page', '--json', '--predecessor-task-id', SECOND_MEMBER))
    ctx.eq((ids(ok['items']), ok.get('successor_scope')), ([], 'recorded_tasks'), 'the task has no successors [baseline]')
    others = success(ctx, ctx.ctl('task-list-page', '--json', '--task-ids', f'{A},{B}'))
    ctx.eq(sorted(ids(others['items'])), sorted([A, B]), 'a page without the task is unaffected [baseline]')
