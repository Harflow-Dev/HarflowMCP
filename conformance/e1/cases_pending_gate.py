"""E1 cases for the pending-launch commands' request gate and for ordinary reads of a pending launch.

Precedence for `task-pending-page` and `task-pending-status` (Python 0.0.13): usage, then environment markers,
then the state root's availability (never initialized), then the direct-only caller gate, then limit, cursor and
task id, then the launch namespace. Every operation error of this family carries `data.coverage: "unknown"`.
"""
from .cases import CORE, agent_caller, case, error, success
from .cases_pending import FIVE, FOUR, ONE, PENDING, RESOURCES, SEVEN, SIX, THREE, TWO, page, status
from .world import A, BROKEN, MISSING, launch, launch_id, numbered_id

CALLER = '34343434-3434-4434-8434-343434343434'
# The legacy-prefixed markers share the detector with the ordinary reads and are covered by `scope.environment_markers`.
MARKERS = ({'HARFLOW_TASK_ID': MISSING}, {'HARFLOW_DEPTH': '1'}, {'HARFLOW_TASK_ID': A}, {'HARFLOW_DEPTH': 'x'})


def refusal(ctx, res, code, check, label):
    err = error(ctx, res, code, label=label)
    expected = {'coverage': 'unknown'} if check is None else {'coverage': 'unknown', 'check': check}
    ctx.eq(err.get('data'), expected, f'{label}: error.data [contract]')


@case('pending-launch.gate_usage_and_markers', PENDING)
def _(ctx):
    ctx.seed_reads()
    ctx.fx.add_launch(launch(ONE, 'reserved'))
    ctx.rebase()
    ctx.eq(ctx.ctl('task-pending-page', '--json', '--limit', 'abc').rc, 2, 'a non-numeric limit is a usage error [contract]')
    ctx.eq(ctx.ctl('task-pending-status', '--json').rc, 2, 'a missing --task-id is a usage error [contract]')
    marker = {'HARFLOW_TASK_ID': MISSING}
    ctx.eq(ctx.ctl('task-pending-page', '--json', '--limit', 'abc', env=marker).rc, 2, 'usage precedes the marker refusal [contract]')
    for env in MARKERS:
        label = ','.join(env)
        for args in ((page, ('--limit', '0')), (status, (ONE,))):
            res = args[0](ctx, *args[1], env=env)
            refusal(ctx, res, 'agent_caller', 'direct_caller_required', f'{args[0].__name__} with {label}')
    # The marker refusal precedes root inspection: a missing root is neither created nor reported.
    ctx.fx.remove_root()
    ctx.baseline = None
    refusal(ctx, page(ctx, env={'HARFLOW_TASK_ID': MISSING}), 'agent_caller', 'direct_caller_required', 'marker on a missing root')
    ctx.eq(ctx.fx.root_state(), 'missing', 'a refused read creates nothing [contract]')
    # An empty marker value still counts as present for this direct-only gate.
    refusal(ctx, page(ctx, env={'HARFLOW_TASK_ID': ''}), 'agent_caller', 'direct_caller_required', 'an empty marker')


@case('pending-launch.gate_root_availability', PENDING)
def _(ctx):
    # Root availability precedes the caller gate's records, limit, cursor and task id checks.
    refusal(ctx, page(ctx, '--limit', '0', '--cursor', 'zz'), 'pending_observation_unavailable', None, 'missing root with a bad limit and cursor')
    refusal(ctx, status(ctx, 'bad id'), 'pending_observation_unavailable', None, 'missing root with an invalid task id')
    refusal(ctx, status(ctx, ONE), 'pending_observation_unavailable', None, 'missing root, exact lookup')
    ctx.eq(ctx.fx.root_state(), 'missing', 'the pending reads never initialize the root [contract]')
    ctx.fx.make_foreign_root()
    ctx.rebase()
    refusal(ctx, page(ctx), 'pending_observation_unavailable', None, 'foreign root')
    refusal(ctx, status(ctx, ONE), 'pending_observation_unavailable', None, 'foreign root, exact lookup')
    ctx.eq(ctx.fx.root_state(), 'other', 'a foreign root is left untouched [contract]')


@case('pending-launch.gate_caller_and_request_order', PENDING)
def _(ctx):
    ctx.seed_reads()
    ctx.fx.add_launch(launch(ONE, 'reserved'))
    ctx.rebase()
    # Direct caller: limit, then cursor, then task id.
    refusal(ctx, page(ctx, '--limit', '0', '--cursor', 'zz'), 'invalid_pending_limit', 'limit_1_to_100', 'limit before cursor')
    refusal(ctx, page(ctx, '--cursor', 'zz'), 'pending_cursor_invalid', 'cursor_shape', 'bad cursor')
    refusal(ctx, status(ctx, 'bad id'), 'invalid_task_id', 'exact_task_id_required', 'invalid task id')
    refusal(ctx, status(ctx, ONE[:8] + '/x'), 'invalid_task_id', 'exact_task_id_required', 'task id that is not a path-safe identity')
    # A verified agent caller is refused before limit, cursor and task id are examined, and learns nothing.
    ctx.eq(agent_caller(ctx, CALLER), True, 'the adapter supports an alive caller (required)')
    for res, label in ((page(ctx, '--limit', '0', '--cursor', 'zz'), 'page'), (status(ctx, 'bad id'), 'status of an invalid id'), (status(ctx, ONE), 'status of a known id')):
        refusal(ctx, res, 'agent_caller', 'direct_caller_required', f'verified agent caller: {label}')
        ctx.check(ONE not in str(res.stdout) and 'Fixture pending launch' not in str(res.stdout), f'{label}: a refused caller sees no launch metadata [contract]')


@case('pending-launch.gate_undecidable_caller', PENDING)
def _(ctx):
    ctx.seed_reads()
    ctx.fx.add_launch(launch(ONE, 'reserved'))
    ctx.rebase()
    ctx.eq(agent_caller(ctx, CALLER, identity='unknown'), True, 'the adapter supports an unknown-identity caller (required)')
    for res, label in ((page(ctx, '--limit', '0'), 'page'), (status(ctx, ONE), 'status')):
        refusal(ctx, res, 'caller_undecidable', 'direct_caller_required', f'undecidable caller: {label}')
        ctx.check(ONE not in str(res.stdout), f'{label}: nothing about the launches is disclosed [contract]')


@case('pending-launch.gate_complete_candidate_qualification', PENDING)
def _(ctx):
    """The direct-caller decision first qualifies every ordinary record; one it cannot qualify makes the caller undecidable."""
    ctx.seed_reads()
    ctx.fx.add_launch(launch(ONE, 'reserved'))
    ctx.fx.damage_task(BROKEN)
    ctx.rebase()
    for res, label in ((page(ctx), 'page'), (status(ctx, ONE), 'status')):
        refusal(ctx, res, 'caller_undecidable', 'direct_caller_required', f'a malformed unrelated record: {label}')
        ctx.check(ONE not in str(res.stdout), f'{label}: no launch metadata is disclosed [contract]')


@case('pending-launch.ordinary_status_of_a_pending_launch', PENDING)
def _(ctx):
    ctx.seed_reads()
    unreasoned, EMPTY_NAMES, EMPTY_PATH = launch_id('8'), numbered_id(300), numbered_id(301)
    specs = [launch(ONE, 'reserved'), launch(TWO, 'waiting_for_resources', resources=RESOURCES),
             launch(THREE, 'granted', resources=RESOURCES), launch(FOUR, 'reserved', worktree={'path': '/tmp/w'}),
             launch(FIVE, 'not_started', resources=RESOURCES, not_started_reason='gave up'),
             launch(unreasoned, 'not_started', resources=RESOURCES), launch(SIX, 'spawned', worktree={'path': '/tmp/w'}),
             launch(SEVEN, 'settled', resources=RESOURCES), launch(EMPTY_NAMES, 'reserved', resources={'names': []}),
             launch(EMPTY_PATH, 'reserved', worktree={'path': ''})]
    for spec in specs:
        ctx.fx.add_launch(spec)
    ctx.rebase()
    pending = {TWO: {'launch_state': 'waiting_for_resources'}, THREE: {'launch_state': 'granted'}, FOUR: {'launch_state': 'reserved'},
               FIVE: {'launch_state': 'not_started', 'not_started_reason': 'gave up'}, unreasoned: {'launch_state': 'not_started'}}
    # Python's status qualifies less than the pending page, which refuses the empty name list: pinned apart as baseline.
    odd = {EMPTY_NAMES: {'launch_state': 'reserved'}, EMPTY_PATH: {'launch_state': 'reserved'}}
    for task_id, data in pending.items():
        err = error(ctx, ctx.ctl('status', task_id, '--json'), 'task_pending_launch', label=f'status {task_id[:4]}')
        ctx.eq(err.get('data'), data, f'status {task_id[:4]}: data carries the launch state [contract]')
        err = error(ctx, ctx.ctl('task-events', task_id, '--json'), 'task_pending_launch', label=f'task-events {task_id[:4]}')
        ctx.eq(err.get('data'), data, f'task-events {task_id[:4]}: data carries the launch state [contract]')
    batch = success(ctx, ctx.ctl('status-batch', '--task-ids', ','.join(pending), '--json'))
    ctx.eq({k: v.get('data') for k, v in batch['errors'].items()}, pending, 'status-batch rows carry the same data as single status [contract]')
    ctx.eq({k: v.get('code') for k, v in batch['errors'].items()}, {k: 'task_pending_launch' for k in pending}, 'status-batch row codes [contract]')
    for task_id, data in odd.items():
        for command in ('status', 'task-events'):
            err = error(ctx, ctx.ctl(command, task_id, '--json'), 'task_pending_launch', label=f'{command} {task_id[:8]} [baseline]')
            ctx.eq(err.get('data'), data, f'{command} {task_id[:8]}: data carries the launch state [baseline]')
    rows = success(ctx, ctx.ctl('status-batch', '--task-ids', ','.join(odd), '--json'))['errors']
    ctx.eq({k: (v.get('code'), v.get('data')) for k, v in rows.items()}, {k: ('task_pending_launch', v) for k, v in odd.items()},
           'status-batch rows of the empty-name and empty-path receipts [baseline]')
    # A launch without resources or a worktree, and spawned or settled history, is not a pending task.
    for task_id in (ONE, SIX, SEVEN):
        error(ctx, ctx.ctl('status', task_id, '--json'), 'unknown_task', label=f'status {task_id[:4]} (not a pending-kind launch)')
    # A qualified ordinary record shadows a receipt with its id.
    ctx.fx.add_launch(launch(A, 'waiting_for_resources', resources=RESOURCES))
    ctx.rebase()
    ctx.eq(success(ctx, ctx.ctl('status', A, '--json'))['task']['task_id'], A, 'an ordinary record wins over a receipt [contract]')
    ctx.eq(success(ctx, status(ctx, A))['disposition'], 'recorded', 'the pending lookup agrees [contract]')
    ctx.fx.add_launch(launch(ONE, 'reserved', resources={}))
    ctx.rebase()
    error(ctx, ctx.ctl('status', ONE, '--json'), 'field_shape', label='status of a receipt with an empty resources object [baseline]')
