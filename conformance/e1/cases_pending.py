"""E1 cases for `task-pending-launch.v1`: bounded launch discovery (`task-pending-page`, `task-pending-status`).

Expectations derive from `docs/contracts.md` ("Pending launch discovery"). Receipts are created only through the
fixture port (`add_launch`, `add_invalid_launch_name`). The pending commands are direct-human-only; the harness runs
them as a verified direct caller unless a case registers one.

Python's page iteration sorts by receipt filename but resumes by bare task id (`task_pending_reads.py:248`), which
skips a launch whose id is a prefix of the previous one (`ab` after `ab-c`: in the filenames `-` sorts below the `.` that starts the suffix). The cases
that cover this shape pin that observed skip under the baseline and the complete traversal under the delta profile
`pending-prefix-paging`.
"""
import base64
import hashlib
import json

from . import harness
from .cases import CORE, case, delta, error, success
from .world import A, B, MISSING, NAMESPACE_SUFFIX, launch, launch_id, numbered_id

PENDING = CORE + ('task-pending-launch.v1',)
SCOPE = 'prerecord_launch_attempts_and_not_started'
PAGE_KEYS = {'scope', 'observed_at', 'items', 'namespace_revision', 'namespace_complete', 'coverage',
             'invalid_name_count', 'examined_count', 'has_more', 'next_cursor', 'bound'}
LAUNCH_KEYS = {'title', 'backend', 'repo_path', 'created_at', 'not_started_reason', 'generation', 'state', 'lineage',
               'resource_names', 'metadata_truncated'}
ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN = (launch_id(c) for c in '1234567')
RESOURCES = {'names': ['fixture']}


def page(ctx, *args, env=None):
    return ctx.ctl('task-pending-page', '--json', *args, env=env)


def status(ctx, task_id, env=None):
    return ctx.ctl('task-pending-status', '--json', '--task-id', task_id, env=env)


def revision(names):
    """The filename-namespace hash: SHA-256 over the sorted receipt filenames, each followed by NUL (`NAMESPACE_SUFFIX`)."""
    digest = hashlib.sha256()
    for name in sorted(f'{name}{NAMESPACE_SUFFIX}' for name in names):
        digest.update(name.encode() + b'\0')
    return digest.hexdigest()


def page_shape(ctx, result, label):
    ctx.eq(set(result), PAGE_KEYS, f'{label}: page fields [contract]')
    ctx.eq((result.get('scope'), result.get('namespace_complete')), (SCOPE, True), f'{label}: scope and namespace_complete [contract]')
    from .cases import now_close
    now_close(ctx, result.get('observed_at'), f'{label}: observed_at')


def rows(result):
    return [(item['task_id'], item['disposition']) for item in result['items']]


def pending_ids(result):
    return [item['task_id'] for item in result['items']]


def follow(ctx, first, *args, limit=50):
    """Every page of a traversal, following cursors; returns the list of page results."""
    pages, result = [], first
    for _ in range(harness.BOOTSTRAP_ATTEMPTS):
        pages.append(result)
        if not result['has_more']:
            return pages
        result = success(ctx, page(ctx, '--limit', str(limit), '--cursor', result['next_cursor'], *args))
    ctx.check(False, 'a pending traversal ends within the harness page budget', tag='harness')
    return pages


CURSOR_SKIP = 'the cursor is not a base64 JSON object whose last position contains the expected id, so position errors cannot be crafted'


def reposition(ctx, cursor, old, new):
    """The opaque cursor with `old` replaced by `new` inside its `last` position (the form of the position is kept).

    Returns None, after recording the named `unverified` skip, when the cursor cannot be decoded that way."""
    try:
        value = json.loads(base64.urlsafe_b64decode(cursor + '=' * (-len(cursor) % 4)))
        if isinstance(value, dict) and isinstance(value.get('last'), str) and old in value['last']:
            value['last'] = value['last'].replace(old, new)
            return base64.urlsafe_b64encode(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).decode()
    except (ValueError, TypeError):
        pass
    ctx.skip(CURSOR_SKIP, name='pending-launch.cursor_position')
    return None


@case('pending-launch.empty_namespace', PENDING)
def _(ctx):
    ctx.seed_reads()
    result = success(ctx, page(ctx))
    page_shape(ctx, result, 'empty namespace')
    ctx.eq((result['items'], result['examined_count'], result['invalid_name_count'], result['has_more'], result['next_cursor'],
            result['bound'], result['coverage']), ([], 0, 0, False, None, None, 'complete'), 'an empty namespace is a complete empty page [contract]')
    ctx.eq(result['namespace_revision'], revision([]), 'the empty namespace has the empty-input hash [baseline]')
    ctx.eq(result['namespace_revision'], hashlib.sha256(b'').hexdigest(), 'the empty-namespace hash is SHA-256 of nothing [baseline]')
    ctx.eq(rows(success(ctx, page(ctx, '--limit', '100'))), [], 'limit 100 is accepted [contract]')
    answer = success(ctx, status(ctx, MISSING))
    ctx.eq((answer['task_id'], answer['disposition'], answer['scope']), (MISSING, 'not_found', SCOPE), 'an unknown id is not_found [contract]')
    ctx.eq(success(ctx, status(ctx, A))['disposition'], 'recorded', 'an ordinary record is recorded [contract]')


@case('pending-launch.states_and_projection', PENDING)
def _(ctx):
    ctx.seed_reads()
    lineage = {'spawned_by': A, 'root_task_id': B, 'parent_task_id': 'not a task id', 'chain_root_task_id': None}
    specs = [launch(ONE, 'reserved'), launch(TWO, 'claimed'),
             launch(THREE, 'waiting_for_resources', resources=RESOURCES),
             launch(FOUR, 'granted', resources={'names': ['b', 'a']}, lineage=lineage),
             launch(FIVE, 'not_started', resources=RESOURCES, not_started_reason='cancelled_before_start'),
             launch(SIX, 'reserved', worktree={'path': '/tmp/harflow-fixture-worktree'}),
             launch(SEVEN, 'not_started', not_started_reason='lost the race')]
    spawned, settled = launch_id('8'), launch_id('9')
    for spec in specs + [launch(spawned, 'spawned', worktree={'path': '/tmp/w'}), launch(settled, 'settled', resources=RESOURCES),
                         launch(A, 'reserved', resources=RESOURCES)]:
        ctx.fx.add_launch(spec)
    ctx.rebase()
    result = success(ctx, page(ctx))
    page_shape(ctx, result, 'states')
    ctx.eq(pending_ids(result), [ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN],
           'pending, claimed, waiting, granted and not_started receipts are listed; spawned, settled and recorded ones are filtered [contract]')
    ctx.eq((result['examined_count'], result['has_more'], result['coverage'], result['bound'], result['invalid_name_count']),
           (10, False, 'complete', None, 0), 'filtered candidates count as examined; nothing uncertain [contract]')
    ctx.eq(result['namespace_revision'], revision([s['name'] for s in specs] + [spawned, settled, A]), 'namespace revision hashes every launch filename [baseline]')
    support = {item['task_id']: item['cancel_support'] for item in result['items']}
    ctx.eq(support, {ONE: 'unsupported', TWO: 'unsupported', THREE: 'supported', FOUR: 'supported', FIVE: 'unsupported',
                     SIX: 'supported', SEVEN: 'unsupported'},
           'cancel_support is supported only for an unfinished resource or worktree launch that is not not_started [contract]')
    by_id = {item['task_id']: item for item in result['items']}
    ctx.eq(by_id[ONE]['launch'], {'title': 'Fixture pending launch', 'backend': 'claude', 'repo_path': '/tmp/harflow-fixture-repo',
                                  'created_at': '2026-11-10T00:00:00+00:00', 'not_started_reason': None, 'generation': 'c' * 32,
                                  'state': 'reserved', 'lineage': {}, 'resource_names': [], 'metadata_truncated': False},
           'a reserved launch projection [contract]')
    ctx.eq(set(by_id[THREE]['launch']), LAUNCH_KEYS, 'launch fields [contract]')
    ctx.eq(by_id[THREE]['launch']['resource_names'], ['fixture'], 'resource names are listed [contract]')
    ctx.eq(by_id[FOUR]['launch']['resource_names'], ['b', 'a'], 'resource names keep the receipt order [baseline]')
    ctx.eq(by_id[FOUR]['launch']['lineage'], {'spawned_by': A, 'root_task_id': B}, 'lineage keeps only valid full identities [contract]')
    ctx.eq(by_id[FIVE]['launch']['not_started_reason'], 'cancelled_before_start', 'not_started_reason is carried [contract]')
    ctx.eq(by_id[SIX]['launch']['resource_names'], [], 'a worktree-only launch has no resource names [contract]')
    ctx.eq(set(by_id[ONE]), {'task_id', 'disposition', 'launch', 'cancel_support'}, 'pending item fields [contract]')
    # The same rows through the exact lookup.
    for task_id in (ONE, THREE, SIX, SEVEN):
        single = success(ctx, status(ctx, task_id))
        ctx.eq({k: v for k, v in single.items() if k not in ('observed_at',)},
               {'scope': SCOPE, 'task_id': task_id, 'disposition': 'pending', 'launch': by_id[task_id]['launch'],
                'cancel_support': by_id[task_id]['cancel_support']}, f'task-pending-status {task_id[:4]} equals its page row [contract]')
    inconsistent = success(ctx, status(ctx, spawned))
    ctx.eq((inconsistent['disposition'], inconsistent.get('code'), inconsistent.get('check')),
           ('unknown', 'pending_record_inconsistent', 'postrecord_source_without_record'), 'spawned without a record is inconsistent [contract]')
    ctx.eq(success(ctx, status(ctx, settled))['disposition'], 'unknown', 'settled without a record is inconsistent [contract]')
    recorded = success(ctx, status(ctx, A))
    ctx.eq((recorded['disposition'], 'launch' in recorded), ('recorded', False), 'a qualified ordinary record wins over a receipt with the same id [contract]')


@case('pending-launch.empty_objects', PENDING)
def _(ctx):
    """A receipt's `resources` or `worktree` object that lacks its required member fails qualification (Python 0.0.13)."""
    ctx.seed_reads()
    specs = {ONE: launch(ONE, 'reserved', resources={}), TWO: launch(TWO, 'reserved', resources={'names': []}),
             THREE: launch(THREE, 'reserved', worktree={}), FOUR: launch(FOUR, 'reserved', worktree={'path': ''})}
    for spec in specs.values():
        ctx.fx.add_launch(spec)
    ctx.rebase()
    result = success(ctx, page(ctx))
    refused = {item['task_id']: (item.get('code'), item.get('check')) for item in result['items'] if item['disposition'] == 'refused'}
    ctx.eq(refused, {ONE: ('field_shape', 'field_shape'), TWO: ('field_shape', 'resource_names'), THREE: ('field_shape', 'field_shape')},
           'empty resources, an empty name list and an empty worktree object are refused rows [baseline]')
    kept = [item for item in result['items'] if item['disposition'] == 'pending']
    ctx.eq([(i['task_id'], i['cancel_support'], i['launch']['resource_names']) for i in kept], [(FOUR, 'supported', [])],
           'a worktree object with a path is a pending-kind launch [baseline]')
    ctx.eq(result['coverage'], 'unknown', 'refused rows leave coverage unknown [contract]')
    for task_id, (code, check) in refused.items():
        answer = success(ctx, status(ctx, task_id))
        ctx.eq((answer['disposition'], answer.get('code'), answer.get('check')), ('refused', code, check), f'exact lookup of {task_id[:4]} [baseline]')


@case('pending-launch.refused_and_invalid_names', PENDING)
def _(ctx):
    ctx.seed_reads()
    checks = (('format', 'format', 'format'), ('version', 'version', 'version'), ('root_mismatch', 'root_mismatch', 'root_mismatch'),
              ('task_id_mismatch', 'task_id_mismatch', 'task_id_mismatch'), ('generation', 'generation', 'generation'),
              ('oversize', 'oversize', 'oversize'), ('field_shape', 'field_shape', 'field_shape'),
              ('resource_names', 'field_shape', 'resource_names'))
    expected = {}
    for index, (kind, code, check) in enumerate(checks):
        task_id = numbered_id(index + 1)
        ctx.fx.add_launch(launch(task_id, 'reserved', refusal={'check': kind}))
        expected[task_id] = (code, check)
    ctx.fx.add_launch(launch(ONE, 'reserved'))
    ctx.fx.add_invalid_launch_name('bad name')
    ctx.fx.add_invalid_launch_name('x.y')
    ctx.rebase()
    result = success(ctx, page(ctx, '--limit', '100'))
    page_shape(ctx, result, 'refused')
    refused = [item for item in result['items'] if item['disposition'] == 'refused']
    for item in refused:
        ctx.eq(set(item), {'task_id', 'disposition', 'code', 'check'}, f'refused row {item["task_id"][:8]} exposes only id, disposition, code and check [contract]')
    ctx.eq({item['task_id']: (item['code'], item['check']) for item in refused}, expected, 'sanitized refusal code and check per qualification failure [contract]')
    ctx.eq(pending_ids(result)[-1], ONE, 'the valid receipt is listed after the numbered ones (filename order) [baseline]')
    ctx.eq((result['invalid_name_count'], result['coverage'], result['examined_count']), (2, 'unknown', 9),
           'invalid names are only counted, and leave coverage unknown; they are not examined [contract]')
    ctx.check('bad name' not in json.dumps(result) and 'x.y' not in json.dumps(result), 'an invalid name is never echoed [contract]')
    answer = success(ctx, status(ctx, numbered_id(1)))
    ctx.eq((answer['disposition'], answer['code'], answer['check']), ('refused', 'format', 'format'), 'exact lookup of a refused receipt [contract]')
    error(ctx, status(ctx, 'bad name'), 'invalid_task_id')


@case('pending-launch.paging', PENDING)
def _(ctx):
    ctx.seed_reads()
    names = [ONE, TWO, THREE, FOUR, FIVE]
    for name in names:
        ctx.fx.add_launch(launch(name, 'reserved'))
    ctx.rebase()
    first = success(ctx, page(ctx, '--limit', '2'))
    page_shape(ctx, first, 'page 1')
    ctx.eq((pending_ids(first), first['has_more'], first['coverage'], first['examined_count']), ([ONE, TWO], True, 'unknown', 2),
           'a continuing page is incomplete coverage [contract]')
    ctx.check(isinstance(first['next_cursor'], str) and first['next_cursor'], 'a continuing page carries a cursor [contract]')
    pages = follow(ctx, first, limit=2)
    ctx.eq([pending_ids(p) for p in pages], [[ONE, TWO], [THREE, FOUR], [FIVE]], 'pages of 2, 2 and 1 in filename order [contract]')
    ctx.eq([(p['has_more'], p['coverage']) for p in pages], [(True, 'unknown'), (True, 'unknown'), (False, 'complete')], 'coverage is complete only on the finished clean traversal [contract]')
    ctx.eq(pages[-1]['next_cursor'], None, 'the final page has no cursor [contract]')
    ctx.eq({p['namespace_revision'] for p in pages}, {revision(names)}, 'every page reports the same namespace revision [baseline]')
    error(ctx, page(ctx, '--limit', '0'), 'invalid_pending_limit')
    error(ctx, page(ctx, '--limit', '101'), 'invalid_pending_limit')
    # Filtered candidates count as examined, and a page skips them until it has `limit` rows to show.
    ctx.fx.add_launch(launch(numbered_id(1), 'spawned'))
    ctx.fx.add_launch(launch(numbered_id(2), 'settled'))
    ctx.rebase()
    window = success(ctx, page(ctx, '--limit', '1'))
    ctx.eq((pending_ids(window), window['examined_count'], window['has_more']), ([ONE], 3, True), 'filtered candidates before the first row count as examined [contract]')
    traversal = follow(ctx, window, limit=1)
    ctx.eq([pending_ids(p) for p in traversal], [[ONE], [TWO], [THREE], [FOUR], [FIVE]], 'one row per page after the filtered ones [contract]')
    ctx.eq([p['examined_count'] for p in traversal], [3, 1, 1, 1, 1], 'examined counts per page [contract]')


@case('pending-launch.refusal_is_sticky', PENDING)
def _(ctx):
    ctx.seed_reads()
    ctx.fx.add_launch(launch(ONE, 'reserved', refusal={'check': 'format'}))
    ctx.fx.add_launch(launch(TWO, 'reserved'))
    ctx.fx.add_launch(launch(THREE, 'reserved'))
    ctx.rebase()
    first = success(ctx, page(ctx, '--limit', '1'))
    ctx.eq(rows(first), [(ONE, 'refused')], 'the refused row is first [contract]')
    pages = follow(ctx, first, limit=1)
    ctx.eq([p['coverage'] for p in pages], ['unknown', 'unknown', 'unknown'],
           'uncertainty seen earlier in a traversal stays in the cursor, so the last page is still unknown [contract]')
    ctx.eq([pending_ids(p) for p in pages], [[ONE], [TWO], [THREE]], 'traversal order')


@case('pending-launch.cursor_lifecycle', PENDING)
def _(ctx):
    ctx.seed_reads()
    for name in (ONE, TWO, THREE):
        ctx.fx.add_launch(launch(name, 'reserved'))
    ctx.rebase()
    first = success(ctx, page(ctx, '--limit', '1'))
    cursor = first['next_cursor']
    # A content-only change keeps the name set, so the cursor stays valid and the new content is shown.
    ctx.fx.add_launch(launch(TWO, 'claimed', title='replaced in place'))
    ctx.rebase()
    second = success(ctx, page(ctx, '--limit', '1', '--cursor', cursor))
    ctx.eq((pending_ids(second), second['items'][0]['launch']['title'], second['items'][0]['launch']['state']), ([TWO], 'replaced in place', 'claimed'),
           'an in-place content change does not expire a cursor and is observed on the next page [contract]')
    ctx.eq(second['namespace_revision'], first['namespace_revision'], 'a content change leaves the namespace hash unchanged [contract]')
    # A row that turns spawned after the cursor was issued is filtered, counts as examined, and the cursor advances.
    ctx.fx.add_launch(launch(TWO, 'spawned'))
    ctx.rebase()
    third = success(ctx, page(ctx, '--limit', '1', '--cursor', cursor))
    ctx.eq((pending_ids(third), third['examined_count'], third['has_more']), ([THREE], 2, False),
           'a row that became spawned is filtered, counts as examined, and the traversal moves past it [contract]')
    # Adding a name expires every older cursor.
    ctx.fx.add_launch(launch(FOUR, 'reserved'))
    ctx.rebase()
    err = error(ctx, page(ctx, '--limit', '1', '--cursor', cursor), 'pending_cursor_expired')
    subset_data(ctx, err, 'namespace_changed')
    fresh = success(ctx, page(ctx, '--limit', '1'))
    ctx.eq(pending_ids(fresh), [ONE], 'a fresh first page works after expiry [contract]')


def subset_data(ctx, err, check):
    ctx.eq(err.get('data'), {'coverage': 'unknown', 'check': check}, f'error data names unknown coverage and the {check} check [contract]')


@case('pending-launch.cursor_errors', PENDING)
def _(ctx):
    ctx.seed_reads()
    for name in (ONE, TWO, THREE):
        ctx.fx.add_launch(launch(name, 'reserved'))
    other = ctx.root.parent / 'state-other'
    ctx.fx.at(other).initialize_empty()
    ctx.fx.at(other).add_launch(launch(ONE, 'reserved'))
    ctx.fx.at(other).add_launch(launch(TWO, 'reserved'))
    ctx.fx.at(other).add_launch(launch(THREE, 'reserved'))
    ctx.rebase()
    cursor = success(ctx, page(ctx, '--limit', '1'))['next_cursor']
    for label, token in (('malformed', 'zz'), ('not base64 JSON', base64.urlsafe_b64encode(b'[1,2]').decode()), ('oversize', 'A' * 5000)):
        err = error(ctx, page(ctx, '--cursor', token), 'pending_cursor_invalid', label=f'cursor: {label}')
        subset_data(ctx, err, 'cursor_shape')
    err = error(ctx, page(ctx, '--limit', '2', '--cursor', cursor), 'pending_cursor_invalid', label='cursor under another limit')
    subset_data(ctx, err, 'cursor_scope')
    err = error(ctx, page(ctx, '--limit', '1', '--cursor', cursor, env={'HARFLOW_STATE_ROOT': str(other)}), 'pending_cursor_invalid',
                label='cursor under another state root')
    subset_data(ctx, err, 'cursor_scope')
    crafted = reposition(ctx, cursor, ONE, launch_id('0'))
    if crafted is not None:
        err = error(ctx, page(ctx, '--limit', '1', '--cursor', crafted), 'pending_cursor_invalid', label='cursor positioned on an unknown name')
        subset_data(ctx, err, 'cursor_position')
    error(ctx, page(ctx, '--limit', '0', '--cursor', 'zz'), 'invalid_pending_limit', label='the limit is checked before the cursor [contract]')


@case('pending-launch.prefix_nested_ids', PENDING)
def _(ctx):
    """Ids where one is a prefix of another: receipt `ab-c` sorts before receipt `ab` by filename."""
    ctx.seed_reads()
    ctx.fx.add_launch(launch('ab', 'reserved'))
    ctx.fx.add_launch(launch('ab-c', 'reserved'))
    ctx.rebase()
    ctx.eq(pending_ids(success(ctx, page(ctx))), ['ab-c', 'ab'], 'a single page lists both in filename order [baseline]')
    first = success(ctx, page(ctx, '--limit', '1'))
    ctx.eq((pending_ids(first), first['has_more']), (['ab-c'], True), 'the first page holds the filename-order first id [baseline]')
    rest = success(ctx, page(ctx, '--limit', '1', '--cursor', first['next_cursor']))
    if delta(ctx, 'pending-prefix-paging'):
        ctx.eq((pending_ids(rest), rest['has_more']), (['ab'], False),
               'resuming after ab-c returns ab: iteration and resumption use the same key [proposed, pending-prefix-paging]')
    else:
        ctx.eq((pending_ids(rest), rest['has_more']), ([], False),
               'Python resumes by bare task id and skips ab, which is a prefix of ab-c [baseline, python bug pending-prefix-paging]')


@case('pending-launch.examined_bound', PENDING)
def _(ctx):
    ctx.seed_reads()
    filtered = [numbered_id(n) for n in range(1, 106)]
    kept = [numbered_id(n) for n in (200, 201)]
    for name in filtered:
        ctx.fx.add_launch(launch(name, 'spawned'))
    for name in kept:
        ctx.fx.add_launch(launch(name, 'reserved'))
    ctx.rebase()
    first = success(ctx, page(ctx, '--limit', '50'))
    ctx.eq((first['items'], first['examined_count'], first['has_more'], first['coverage'], first['bound']), ([], 100, True, 'unknown', None),
           'a page examines at most 100 candidates, filtered ones included [contract]')
    second = success(ctx, page(ctx, '--limit', '50', '--cursor', first['next_cursor']))
    ctx.eq((pending_ids(second), second['examined_count'], second['has_more']), (kept, 7, False),
           'the next page resumes after the last examined candidate [contract]')
    ctx.eq(second['coverage'], 'complete', 'stopping at the examined bound is not itself sticky uncertainty [baseline]')


@case('pending-launch.truncation_and_response_bound', PENDING)
def _(ctx):
    ctx.seed_reads()
    euro = '€'   # three UTF-8 bytes: a byte cut lands inside the character
    ctx.fx.add_launch(launch(ONE, 'reserved', title=euro * 400, repo_path=euro * 1500, not_started_reason=euro * 400))
    result = success(ctx, page(ctx))
    item = result['items'][0]['launch']
    ctx.eq(item['title'], euro * 341, 'a text field is cut to 1024 bytes on a character boundary [contract]')
    ctx.eq(item['repo_path'], euro * 1365, 'the repository path is cut to 4096 bytes on a character boundary [contract]')
    ctx.eq((item['not_started_reason'], item['metadata_truncated']), (euro * 341, True), 'truncation is reported [contract]')
    big = [numbered_id(n) for n in range(1, 31)]
    for name in big:
        ctx.fx.add_launch(launch(name, 'reserved', repo_path='p' * 4096, title='t' * 1024))
    ctx.fx.add_launch(launch(ONE, 'reserved', title='back to normal'))
    ctx.rebase()
    res = page(ctx, '--limit', '100')
    first = success(ctx, res)
    count = len(first['items'])
    ctx.check(0 < count < 30, f'the response size bound stops the page before 30 rows, got {count} [contract]')
    ctx.eq((first['bound'], first['has_more'], first['coverage']), ('response_bytes', True, 'unknown'), 'the response bound is reported with unknown coverage [contract]')
    ctx.check(len(res.stdout) < 128 * 1024, 'the command output stays below 128 KiB [contract]')
    seen = []
    for result in follow(ctx, first, limit=100):
        seen += pending_ids(result)
    ctx.eq(seen, big + [ONE], 'continuing the cursor loses no row [contract]')
