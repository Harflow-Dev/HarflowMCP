"""E1 read cases. Expectations are literals derived from `seed.py` and the published contract.

Each case runs in a fresh isolated state root against the public command line only. Checks tagged
`[contract]` follow `docs/contracts.md`; checks tagged `[baseline]` pin observed 0.0.11 behaviour that
the contracts do not spell out, so an engine that differs must record the difference as a contract
delta (C1) rather than silently diverge.
"""
import hashlib
import json
import os
import subprocess
import time
from datetime import datetime

from . import seed
from .seed import A, B, C, D, E, MISSING

CASES = {}
CORE = ('ctl.v1',)


def case(name, capabilities=CORE):
    def wrap(function):
        CASES[name] = (capabilities, function)
        return function
    return wrap


def epoch(text):
    return datetime.fromisoformat(text).timestamp()


def error(ctx, res, code, rc=1, label=''):
    """The command failed with `code`: a JSON error document on stdout and the stated exit status."""
    name = label or ' '.join(map(str, res.args[:2]))
    ctx.eq(res.rc, rc, f'{name}: exit status')
    got = (res.error or {}).get('code') if res.doc else None
    ctx.eq(got, code, f'{name}: error.code')
    return res.error or {}


def success(ctx, res, label=''):
    name = label or ' '.join(map(str, res.args[:2]))
    ctx.eq(res.rc, 0, f'{name}: exit status')
    ctx.check(res.doc is not None and 'error' not in res.doc, f'{name}: no error document')
    return res.result if res.doc and 'result' in res.doc else (res.doc or {})


def subset(ctx, actual, expected, label):
    for key, value in expected.items():
        ctx.eq(actual.get(key, '<absent>'), value, f'{label}.{key}')


def now_close(ctx, text, label):
    try:
        delta = abs(datetime.fromisoformat(text).timestamp() - time.time())
        ctx.check(delta < 600, f'{label} is the current UTC time')
    except (TypeError, ValueError):
        ctx.check(False, f'{label} is an ISO8601 timestamp, got {text!r}')


def agent_caller(ctx, task_id):
    """Register the test process itself as a live task's process: the engine, being its descendant,
    is run by a verified agent caller (detected by ancestry)."""
    out = subprocess.run(['ps', '-o', 'lstart=', '-p', str(os.getpid())], capture_output=True, text=True,
                         env={**os.environ, 'LC_ALL': 'C'}).stdout
    seed.write_task(ctx.tasks, task_id, status='running', exit_code=None, finished_at=None, output_drained=False,
                    pid=os.getpid(), start_time=' '.join(out.split()), started_at='2026-11-07T00:00:00+00:00',
                    prompt='the caller')
    ctx.rebase()


CALLER = '12121212-1212-4212-8212-121212121212'
WINDOW = ['--since', '2026-01-01T00:00:00+00:00', '--before', '2026-02-01T00:00:00+00:00']
ALL_IDS = {A, B, C, D, E}


def pending_shape(ctx, result, label):
    ctx.eq(result.get('items'), [], f'{label}: preparing page has no items [contract]')
    ctx.eq(result.get('related_headers'), [], f'{label}: preparing page has no related headers [contract]')
    ctx.eq(result.get('next_cursor'), None, f'{label}: preparing page has no cursor [contract]')
    ctx.eq(result.get('has_more'), False, f'{label}: preparing page has_more [contract]')
    ctx.eq(result.get('counts_complete'), False, f'{label}: preparing page counts_complete [contract]')
    state = result.get('catalog_state')
    ctx.check(isinstance(state, dict) and set(state) <= {'status', 'source', 'reason'},
              f'{label}: catalog_state exposes only status, source, reason [contract]')


def page_ready(ctx, *args, attempts=40, env=None):
    """Re-issue a page read while it reports bootstrap_pending, as the contract tells clients to."""
    for _ in range(attempts):
        res = ctx.ctl(*args, env=env)
        result = success(ctx, res)
        if result.get('bootstrap_pending') is not True:
            return res
        pending_shape(ctx, result, 'task-list-page')
        time.sleep(0.25)
    ctx.check(False, 'task-list-page leaves bootstrap_pending within the bounded retries')
    return res


def ids(items):
    return [row['task_id'] for row in items]


# --- list ---------------------------------------------------------------------------------------------

@case('list.populated')
def _(ctx):
    ctx.seed_reads()
    doc = success(ctx, ctx.ctl('list', '--json'))
    ctx.eq({row['task_id'] for row in doc['tasks']}, ALL_IDS, 'list returns every seeded task exactly once [contract]')
    ctx.eq(len(doc['tasks']), 5, 'list has no duplicate rows')
    ctx.eq(doc.get('history_incomplete'), False, 'history_incomplete [contract]')
    ctx.eq(doc.get('refused_tasks'), [], 'refused_tasks [contract]')
    rows = {row['task_id']: row for row in doc['tasks']}
    subset(ctx, rows[A], {'backend': 'claude', 'status': 'completed', 'repo_path': '/workspace/repo',
                          'started_at': '2026-01-10T10:00:00+00:00', 'title': 'First task'}, 'list[A]')
    subset(ctx, rows[B], {'status': 'failed', 'title': 'Second task'}, 'list[B]')
    subset(ctx, rows[C], {'status': 'completed', 'title': 'Third task'}, 'list[C]')


@case('list.since')
def _(ctx):
    ctx.seed_reads()
    recent = '77777777-7777-4777-8777-777777777777'
    stamp = datetime.fromtimestamp(time.time() - 3600).astimezone().isoformat()
    seed.write_task(ctx.tasks, recent, started_at=stamp, finished_at=stamp, prompt='recent')
    ctx.rebase()
    doc = success(ctx, ctx.ctl('list', '--json', '--since', '7d'))
    ctx.eq([row['task_id'] for row in doc['tasks']], [recent], 'only a task started within the window is listed [contract]')
    doc = success(ctx, ctx.ctl('list', '--json', '--since', '10000d'))
    ctx.eq({row['task_id'] for row in doc['tasks']}, ALL_IDS | {recent}, 'a wide window lists every task')
    error(ctx, ctx.ctl('list', '--json', '--since', 'bogus'), 'usage', rc=2)


@case('list.missing_root_initializes')
def _(ctx):
    ctx.eq(ctx.root.exists(), False, 'precondition: no state root')
    doc = success(ctx, ctx.ctl('list', '--json'))
    ctx.eq(doc['tasks'], [], 'an empty store lists no tasks')
    marker = json.loads((ctx.root / '.harflow-store.json').read_text())
    subset(ctx, marker, {'product': 'harflow', 'format': 'harflow.store', 'v': 1, 'layout_version': 1}, 'marker [contract]')
    ctx.check((ctx.root / '.harflow-store.lock').exists(), 'the stable store lock exists [contract]')


@case('list.foreign_root_refused')
def _(ctx):
    ctx.root.mkdir()
    (ctx.root / 'stranger.txt').write_text('not harflow')
    res = ctx.ctl('list', '--json')
    err = error(ctx, res, 'state_incompatible')
    subset(ctx, err.get('data') or {}, {'reason': 'unmarked_nonempty', 'state_root': str(ctx.root)}, 'error.data [contract]')
    ctx.eq(sorted(os.listdir(ctx.root)), ['stranger.txt'], 'a refused root is left untouched [contract]')


# --- status / status-batch ----------------------------------------------------------------------------

@case('status.exact_and_prefix')
def _(ctx):
    ctx.seed_reads()
    task = success(ctx, ctx.ctl('status', A, '--json'))['task']
    subset(ctx, task, {'task_id': A, 'status': 'completed', 'exit_code': 0, 'backend': 'claude',
                       'session_id': 'session-aaaaaaaa', 'repo_path': '/workspace/repo',
                       'started_at': '2026-01-10T10:00:00+00:00', 'title': 'First task', 'group': 'g1',
                       'model': 'model-a', 'freedom': 'write_in_repo', 'duration_seconds': 300.0,
                       'output_drained': True, 'continuation_of_task_id': None}, 'status')
    ctx.check('resolved_from' not in task, 'a full id carries no resolved_from [contract]')
    task = success(ctx, ctx.ctl('status', 'aaaaaaaa', '--json'))['task']
    ctx.eq(task['task_id'], A, 'a unique 8-character prefix resolves [contract]')
    ctx.eq(task.get('resolved_from'), {'input': 'aaaaaaaa', 'task_id': A}, 'prefix echo [contract]')
    task = success(ctx, ctx.ctl('status', C, '--json'))['task']
    subset(ctx, task, {'continuation_of_task_id': A, 'chain_root_task_id': A}, 'status[C]')
    task = success(ctx, ctx.ctl('status', B, '--json'))['task']
    subset(ctx, task, {'status': 'failed', 'exit_code': 1}, 'status[B]')


@case('status.refusals')
def _(ctx):
    ctx.seed_reads()
    for name in ('abababab-0000-4000-8000-000000000001', 'abababab-1111-4111-8111-111111111111'):
        seed.write_task(ctx.tasks, name)
    ctx.rebase()
    err = error(ctx, ctx.ctl('status', MISSING, '--json'), 'unknown_task')
    ctx.eq((err.get('data') or {}).get('input'), MISSING, 'unknown id is echoed in data.input [contract]')
    err = error(ctx, ctx.ctl('status', 'zz', '--json'), 'unknown_task')
    subset(ctx, err.get('data') or {}, {'reason': 'prefix_too_short', 'min_prefix_length': 8}, 'short prefix data [contract]')
    err = error(ctx, ctx.ctl('status', 'nomatch0123', '--json'), 'unknown_task')
    ctx.eq((err.get('data') or {}).get('reason'), 'no_prefix_match', 'unmatched prefix reason [contract]')
    err = error(ctx, ctx.ctl('status', 'abababab', '--json'), 'ambiguous_task_id')
    data = err.get('data') or {}
    ctx.eq(sorted(row['task_id'] for row in data.get('candidates', [])),
           ['abababab-0000-4000-8000-000000000001', 'abababab-1111-4111-8111-111111111111'], 'ambiguous candidates [contract]')
    error(ctx, ctx.ctl('status', 'bad/id', '--json'), 'invalid_task_id')


@case('status-batch.rows_and_errors')
def _(ctx):
    ctx.seed_reads()
    argument = ','.join([A, MISSING, 'bad/id', 'bbbbbbbb', B, A])
    doc = success(ctx, ctx.ctl('status-batch', '--task-ids', argument, '--json'))
    rows = doc['tasks']
    ctx.eq([row['task_id'] for row in rows], [A, B, B],
           'one row per distinct input in order; two spellings of one task give two rows [contract]')
    ctx.eq(rows[1].get('resolved_from'), {'input': 'bbbbbbbb', 'task_id': B}, 'prefix row echoes resolved_from [contract]')
    ctx.check('resolved_from' not in rows[2], 'exact row has no resolved_from')
    ctx.eq(sorted(doc['errors']), sorted([MISSING, 'bad/id']), 'errors are keyed by the raw input [contract]')
    ctx.eq(doc['errors'][MISSING]['code'], 'unknown_task', 'unknown id code uses the status code [contract]')
    ctx.eq(doc['errors']['bad/id']['code'], 'invalid_task_id', 'invalid id code [contract]')
    ctx.eq(rows[0]['status'], 'completed', 'batch row carries the status snapshot')
    doc = success(ctx, ctx.ctl('status-batch', '--task-ids', A, '--json'))
    ctx.eq((len(doc['tasks']), doc['errors']), (1, {}), 'a single id answers one row and no errors')


@case('status-batch.bounds')
def _(ctx):
    ctx.seed_reads()
    many = ','.join(f'x{n}' for n in range(101))
    error(ctx, ctx.ctl('status-batch', '--task-ids', many, '--json'), 'invalid_params')
    hundred = ','.join([A] + [f'x{n}' for n in range(99)])
    doc = success(ctx, ctx.ctl('status-batch', '--task-ids', hundred, '--json'))
    ctx.eq((len(doc['tasks']), len(doc['errors'])), (1, 99), 'exactly 100 ids are answered [contract]')


# --- task-list-page -----------------------------------------------------------------------------------

@case('task-list-page.paging')
def _(ctx):
    ctx.seed_reads()
    first = page_ready(ctx, 'task-list-page', '--json', '--limit', '2')
    result = first.result
    ctx.eq(ids(result['items']), [E, D], 'first page is the two newest tasks, newest first [baseline]')
    ctx.eq(result['has_more'], True, 'first page has_more')
    ctx.eq(result.get('counts_complete'), True, 'ready page reports counts_complete')
    ctx.eq(result.get('total_active_count'), 0, 'no active tasks are counted')
    seen = ids(result['items'])
    cursor = result['next_cursor']
    ctx.check(isinstance(cursor, str) and cursor, 'a continuing page carries an opaque next_cursor')
    for expected, more in (([C, B], True), ([A], False)):
        res = ctx.ctl('task-list-page', '--json', '--limit', '2', '--cursor', cursor)
        page = success(ctx, res)
        ctx.eq(ids(page['items']), expected, 'continuation page items [baseline]')
        ctx.eq(page['has_more'], more, 'continuation has_more')
        seen += ids(page['items'])
        cursor = page['next_cursor']
    ctx.eq(cursor, None, 'the last page has no next_cursor [contract]')
    ctx.eq(seen, [E, D, C, B, A], 'pages are disjoint and cover the history [contract]')
    error(ctx, ctx.ctl('task-list-page', '--json', '--cursor', 'bogus'), 'invalid_params')
    error(ctx, ctx.ctl('task-list-page', '--json', '--limit', '0'), 'invalid_params')
    error(ctx, ctx.ctl('task-list-page', '--json', '--limit', '101'), 'invalid_params')
    row = success(ctx, ctx.ctl('task-list-page', '--json', '--limit', '100'))['items']
    ctx.eq(ids(row), [E, D, C, B, A], 'limit 100 returns the whole short history')
    subset(ctx, [r for r in row if r['task_id'] == A][0], {'status': 'completed', 'title': 'First task', 'backend': 'claude',
                                                          'continuation_of_task_id': None}, 'header[A]')


@case('task-list-page.filters')
def _(ctx):
    ctx.seed_reads()
    page_ready(ctx, 'task-list-page', '--json')
    result = success(ctx, ctx.ctl('task-list-page', '--json', '--task-ids', f'{A},{MISSING},cccccccc'))
    ctx.eq(sorted(ids(result['items'])), sorted([A, C]), 'explicit ids: a missing canonical id is dropped silently [contract]')
    ctx.eq(result.get('resolved_from_task_ids'), [{'input': 'cccccccc', 'task_id': C}], 'only prefix entries are echoed [contract]')
    err = error(ctx, ctx.ctl('task-list-page', '--json', '--task-ids', 'not-a-real-one-1'), 'unknown_task')
    ctx.eq((err.get('data') or {}).get('reason'), 'no_prefix_match', 'a missing non-canonical id is reported [contract]')
    result = success(ctx, ctx.ctl('task-list-page', '--json', '--active-only'))
    ctx.eq((ids(result['items']), result.get('total_active_count')), ([], 0), '--active-only lists no settled task')
    result = success(ctx, ctx.ctl('task-list-page', '--json', '--session-id', 'session-cccccccc'))
    ctx.eq(ids(result['items']), [C], '--session-id filters by recorded session')


@case('task-list-page.successors', ('ctl.v1', 'task-successors.v1'))
def _(ctx):
    ctx.seed_reads()
    page_ready(ctx, 'task-list-page', '--json')
    result = success(ctx, ctx.ctl('task-list-page', '--json', '--predecessor-task-id', 'aaaaaaaa'))
    ctx.eq(ids(result['items']), [C], 'successors of A are its recorded continuations [contract]')
    ctx.eq(ids(result['related_headers']), [A], 'the predecessor header is returned [contract]')
    subset(ctx, result, {'predecessor_task_id': A, 'successor_scope': 'recorded_tasks'}, 'successor scope [contract]')
    ctx.eq(result.get('resolved_from_predecessor_task_id'), {'input': 'aaaaaaaa', 'task_id': A}, 'predecessor prefix echo [contract]')
    result = success(ctx, ctx.ctl('task-list-page', '--json', '--predecessor-task-id', C))
    ctx.eq((ids(result['items']), result.get('successor_scope')), ([], 'recorded_tasks'), 'a task without successors lists none [contract]')
    error(ctx, ctx.ctl('task-list-page', '--json', '--predecessor-task-id', MISSING), 'unknown_task')
    error(ctx, ctx.ctl('task-list-page', '--json', '--predecessor-task-id', A, '--task-ids', A), 'usage', rc=2)


# --- task-events --------------------------------------------------------------------------------------

EV = ('task-event-page.v1',)


def seqs(result):
    return [event['seq'] for event in result['events']]


@case('task-events.newest_and_final', CORE + EV)
def _(ctx):
    ctx.seed_reads()
    result = success(ctx, ctx.ctl('task-events', A, '--json', '--limit', '3'))
    subset(ctx, result, {'task_id': A, 'order': 'ascending', 'kinds': 'default', 'has_older': True, 'has_newer': False,
                         'gaps': [], 'skipped': {'malformed': 0, 'oversized': 0}, 'span': {'first_seq': 5, 'last_seq': 7}}, 'newest page [contract]')
    ctx.eq(seqs(result), [5, 6, 7], 'newest page is the last three default-kind events, ascending [contract]')
    ctx.eq(result['log'], {'state': 'final', 'task_status': 'completed', 'output_drained': True,
                           'capture_state': 'observed_terminal', 'final_seq': 7}, 'log state final [contract]')
    ctx.check(all(isinstance(result.get(k), str) and result[k] for k in ('older_cursor', 'newer_cursor', 'change_token')),
              'cursors and change_token are opaque non-empty strings [contract]')
    now_close(ctx, result['observed_at'], 'observed_at')
    for event in result['events']:
        subset(ctx, event, {'product': 'harflow', 'format': 'harflow.event', 'v': 1}, f'event {event["seq"]} envelope [contract]')
    final = result['events'][-1]
    subset(ctx, final, {'kind': 'task_finished', 'status': 'completed', 'exit_code': 0}, 'task_finished event')
    ctx.eq(seqs(success(ctx, ctx.ctl('task-events', A, '--json'))), [0, 3, 4, 5, 6, 7],
           'default kinds hide assistant_delta while keeping seq values [contract]')
    prefixed = success(ctx, ctx.ctl('task-events', 'aaaaaaaa', '--json'))
    ctx.eq(prefixed.get('resolved_from'), {'input': 'aaaaaaaa', 'task_id': A}, 'prefix echo [contract]')


@case('task-events.paging_roundtrip', CORE + EV)
def _(ctx):
    ctx.seed_reads()
    page = success(ctx, ctx.ctl('task-events', A, '--json', '--from', 'oldest', '--limit', '2'))
    ctx.eq((seqs(page), page['has_older'], page['older_cursor'], page['has_newer']), ([0], False, None, True),
           'oldest-forward first page: older_cursor is null at the start of the log [contract]')
    ctx.eq(page['span'], {'first_seq': 0, 'last_seq': 1}, '--limit bounds decoded lines, hidden ones included [contract]')
    collected, spans = list(seqs(page)), [page['span']]
    while page['has_newer']:
        page = success(ctx, ctx.ctl('task-events', A, '--json', '--cursor', page['newer_cursor'], '--limit', '2'))
        collected += seqs(page)
        spans.append(page['span'])
        ctx.check(len(spans) < 10, 'paging terminates')
    ctx.eq(collected, [0, 3, 4, 5, 6, 7], 'forward paging yields every default-kind event once [contract]')
    ctx.check(all(a['last_seq'] + 1 == b['first_seq'] for a, b in zip(spans, spans[1:]) if a and b),
              'adjacent pages do not overlap and leave no unexplained gap [contract]')
    poll = success(ctx, ctx.ctl('task-events', A, '--json', '--cursor', page['newer_cursor'], '--limit', '2'))
    ctx.eq((poll['events'], poll['has_newer'], poll['newer_cursor']), ([], False, page['newer_cursor']),
           'polling with nothing new returns an empty page with an equal cursor [contract]')
    older = success(ctx, ctx.ctl('task-events', A, '--json', '--cursor', page['older_cursor'], '--limit', '2'))
    ctx.eq(older['span']['last_seq'] + 1, page['span']['first_seq'], 'an older cursor reads the page just before [contract]')
    back = success(ctx, ctx.ctl('task-events', A, '--json', '--cursor', older['newer_cursor'], '--limit', '2'))
    ctx.eq(seqs(back), seqs(page), 'reversing direction returns the page just left [contract]')


@case('task-events.kind_filters', CORE + EV)
def _(ctx):
    ctx.seed_reads()
    result = success(ctx, ctx.ctl('task-events', A, '--json', '--all-kinds', '--from', 'oldest', '--limit', '200'))
    ctx.eq((seqs(result), result['kinds']), ([0, 1, 2, 3, 4, 5, 6, 7], 'all'), '--all-kinds adds assistant_delta [contract]')
    result = success(ctx, ctx.ctl('task-events', A, '--json', '--from', 'oldest', '--kinds', 'tool_result,tool_call'))
    ctx.eq((seqs(result), result['kinds']), ([4, 5], ['tool_call', 'tool_result']), '--kinds filters and reports the sorted list [contract]')
    ctx.eq(result['span'], {'first_seq': 0, 'last_seq': 7}, 'span counts filtered lines too [contract]')
    ctx.eq(result['gaps'], [], 'filtered lines are not gaps [contract]')
    result = success(ctx, ctx.ctl('task-events', A, '--json', '--kinds', 'assistant_delta', '--from', 'oldest'))
    ctx.eq(seqs(result), [1, 2], 'assistant_delta can be named explicitly [contract]')


@case('task-events.log_states', CORE + EV)
def _(ctx):
    ctx.seed_reads()
    result = success(ctx, ctx.ctl('task-events', B, '--json', '--from', 'oldest'))
    ctx.eq(seqs(result), [0, 1, 2], 'unfinished log is readable')
    ctx.eq(result['log'], {'state': 'incomplete', 'task_status': 'failed', 'output_drained': True,
                           'capture_state': 'observed_terminal', 'final_seq': None},
           'a finished task whose log lacks task_finished is incomplete after the grace period [contract]')
    result = success(ctx, ctx.ctl('task-events', C, '--json'))
    ctx.eq((result['events'], result['span'], result['older_cursor'], result['newer_cursor'], result['has_older'], result['has_newer']),
           ([], None, None, None, False, False), 'a task without a log is unavailable with no cursors [contract]')
    ctx.eq(result['log']['state'], 'unavailable', 'log.state unavailable [contract]')
    gappy = '12345678-0000-4000-8000-000000000001'
    seed.write_task(ctx.tasks, gappy)
    lines = [seed.event(0, 'task_started', gappy, '2026-01-10T10:00:00+00:00'),
             seed.event(1, 'assistant_text', gappy, '2026-01-10T10:00:01+00:00', text='a')]
    body = ''.join(json.dumps(line) + '\n' for line in lines) + 'not json\n'
    body += json.dumps(seed.event(5, 'assistant_text', gappy, '2026-01-10T10:00:02+00:00', text='b')) + '\n'
    body += json.dumps(seed.event(6, 'task_finished', gappy, '2026-01-10T10:00:03+00:00', status='completed', exit_code=0)) + '\n'
    seed._write(ctx.tasks / f'{gappy}.events.jsonl', body)
    ctx.rebase()
    result = success(ctx, ctx.ctl('task-events', gappy, '--json', '--from', 'oldest'))
    ctx.eq(seqs(result), [0, 1, 5, 6], 'a malformed line is skipped, not fatal [contract]')
    ctx.eq(result['skipped'], {'malformed': 1, 'oversized': 0}, 'malformed lines are counted [contract]')
    ctx.eq(result['gaps'], [{'after_seq': 1, 'before_seq': 5, 'reason': 'unreadable_records'}],
           'seq values no parsed line accounts for are a gap, unreadable_records when skipped lines lie between [contract]')
    ctx.eq(result['log']['state'], 'final', 'log state final despite a skipped line [contract]')


@case('task-events.append_and_cursor_expiry', CORE + EV)
def _(ctx):
    ctx.seed_reads()
    path = ctx.tasks / f'{A}.events.jsonl'
    original = path.read_bytes()
    page = success(ctx, ctx.ctl('task-events', A, '--json', '--from', 'oldest', '--limit', '4'))
    cursor = page['newer_cursor']
    line = json.dumps(seed.event(8, 'assistant_text', A, '2026-01-10T10:06:00+00:00', text='late')) + '\n'
    with open(path, 'ab') as stream:
        stream.write(line.encode())
        stream.write(b'{"partial":')
    ctx.rebase()
    after = success(ctx, ctx.ctl('task-events', A, '--json', '--cursor', cursor, '--limit', '50'))
    ctx.eq(seqs(after), [4, 5, 6, 7, 8], 'an append does not expire the cursor and a partial trailing line is held back [contract]')
    ctx.eq(after['has_newer'], False, 'a partial trailing line never sets has_newer [contract]')
    ctx.eq(after['log']['state'], 'incomplete', 'a log whose last complete line is not task_finished is not final [contract]')
    path.write_bytes(original[:100])
    ctx.rebase()
    err = error(ctx, ctx.ctl('task-events', A, '--json', '--cursor', cursor), 'event_cursor_expired')
    ctx.eq(err.get('data'), {'reload': 'newest'}, 'truncation expires the cursor with reload hint [contract]')
    path.unlink()
    ctx.rebase()
    error(ctx, ctx.ctl('task-events', A, '--json', '--cursor', cursor), 'event_cursor_expired')
    result = success(ctx, ctx.ctl('task-events', A, '--json'))
    ctx.eq((result['log']['state'], result['events']), ('unavailable', []), 'a vanished log reads as unavailable [contract]')


@case('task-events.refusals_and_usage', CORE + EV)
def _(ctx):
    ctx.seed_reads()
    page = success(ctx, ctx.ctl('task-events', A, '--json', '--from', 'oldest', '--limit', '2'))
    cursor = page['newer_cursor']
    err = error(ctx, ctx.ctl('task-events', A, '--json', '--cursor', 'garbage'), 'event_cursor_invalid')
    ctx.eq(err.get('data'), {'reload': 'newest'}, 'invalid cursor carries the reload hint [contract]')
    error(ctx, ctx.ctl('task-events', B, '--json', '--cursor', cursor), 'event_cursor_invalid', label='another task\'s cursor')
    error(ctx, ctx.ctl('task-events', A, '--json', '--cursor', cursor, '--kinds', 'usage'), 'event_cursor_invalid', label='changed filter')
    error(ctx, ctx.ctl('task-events', A, '--json', '--cursor', 'x' * 2049), 'event_cursor_invalid', label='oversize cursor')
    error(ctx, ctx.ctl('task-events', MISSING, '--json'), 'unknown_task')
    error(ctx, ctx.ctl('task-events', 'zz', '--json'), 'unknown_task')
    error(ctx, ctx.ctl('task-events', 'bad/id', '--json'), 'invalid_task_id')
    ctx.eq(ctx.ctl('task-events', A, '--limit', '3').rc, 2, 'missing --json is a usage error [contract]')
    for args in (['--limit', '0'], ['--limit', '201'], ['--kinds', 'bogus'], ['--kinds', 'usage', '--all-kinds'],
                 ['--from', 'oldest', '--cursor', cursor], ['--from', 'sideways']):
        error(ctx, ctx.ctl('task-events', A, '--json', *args), 'usage', rc=2, label=f'task-events {" ".join(args)}')


# --- task-prompt --------------------------------------------------------------------------------------

PR = ('task-prompt.v1',)


@case('task-prompt.states_and_bounds', CORE + PR)
def _(ctx):
    ctx.seed_reads()
    mismatch = '12345678-0000-4000-8000-0000000000f1'
    digest = hashlib.sha256(seed.LONG_PROMPT.encode()).hexdigest()
    name = f'{mismatch}.prompt.{digest}.txt'
    seed.write_task(ctx.tasks, mismatch, prompt=seed.LONG_PROMPT[:2000], prompt_source=name, prompt_truncated=True)
    seed._write(ctx.tasks / name, seed.LONG_PROMPT + 'tampered')
    foreign = '12345678-0000-4000-8000-0000000000f2'
    seed.write_task(ctx.tasks, foreign, prompt=seed.LONG_PROMPT[:2000], prompt_source='bad-name.txt', prompt_truncated=True)
    ctx.rebase()
    result = success(ctx, ctx.ctl('task-prompt', A, '--json'))
    ctx.eq((result['task_id'], result['title']), (A, 'First task'), 'task-prompt identity')
    ctx.eq(result['prompt'], {'state': 'available', 'text': 'Summarize the repo.', 'returned_bytes': 19, 'total_bytes': 19,
                              'truncated': False, 'complete': True, 'integrity': 'not_applicable'}, 'inline prompt [contract]')
    full = seed.LONG_PROMPT
    result = success(ctx, ctx.ctl('task-prompt', D, '--json'))
    ctx.eq(result['prompt'], {'state': 'available', 'text': full, 'returned_bytes': len(full.encode()), 'total_bytes': len(full.encode()),
                              'truncated': False, 'complete': True, 'integrity': 'verified'}, 'sidecar prompt is verbatim and verified [contract]')
    cut = success(ctx, ctx.ctl('task-prompt', D, '--json', '--max-bytes', '11'))['prompt']
    head = full.encode()[:11].decode('utf-8', 'ignore')
    ctx.eq(cut, {'state': 'available', 'text': head, 'returned_bytes': len(head.encode()), 'total_bytes': len(full.encode()),
                 'truncated': True, 'complete': False, 'integrity': 'verified'}, 'a cut drops only an incomplete trailing character [contract]')
    ctx.eq(len(head.encode()), 10, 'self-check: the multibyte character straddles the bound')
    one = success(ctx, ctx.ctl('task-prompt', D, '--json', '--max-bytes', '1'))['prompt']
    ctx.eq((one['text'], one['returned_bytes'], one['truncated']), ('l', 1, True), '--max-bytes 1 [contract]')
    preview = success(ctx, ctx.ctl('task-prompt', E, '--json'))['prompt']
    ctx.eq((preview['state'], preview['text'], preview['complete'], preview['total_bytes'], preview['integrity']),
           ('preview_only', full[:2000], False, None, 'not_applicable'), 'missing sidecar gives the non-authoritative preview [contract]')
    failed = success(ctx, ctx.ctl('task-prompt', mismatch, '--json'))['prompt']
    ctx.eq((failed['state'], failed['text'], failed['integrity'], failed['complete']), ('integrity_failed', None, 'failed', False),
           'a sidecar not matching its digest is integrity_failed with no text [contract]')
    other = success(ctx, ctx.ctl('task-prompt', foreign, '--json'))['prompt']
    ctx.eq((other['state'], other['text'], other['complete']), ('unavailable', None, False), 'a sidecar name not bound to the task is unavailable [contract]')
    ctx.eq(success(ctx, ctx.ctl('task-prompt', 'dddddddd', '--json')).get('resolved_from'), {'input': 'dddddddd', 'task_id': D}, 'prefix echo [contract]')


@case('task-prompt.errors', CORE + PR)
def _(ctx):
    ctx.seed_reads()
    error(ctx, ctx.ctl('task-prompt', MISSING, '--json'), 'unknown_task')
    error(ctx, ctx.ctl('task-prompt', A, '--json', '--max-bytes', '0'), 'usage', rc=2)
    error(ctx, ctx.ctl('task-prompt', A, '--json', '--max-bytes', '131073'), 'usage', rc=2)
    ctx.eq(ctx.ctl('task-prompt', A).rc, 2, 'missing --json is a usage error [contract]')
    result = success(ctx, ctx.ctl('task-prompt', A, '--json', '--max-bytes', '131072'))
    ctx.eq(result['prompt']['state'], 'available', '131072 is the inclusive upper bound [contract]')


# --- task-artifacts -----------------------------------------------------------------------------------

def artifact_item(task_id, artifact_id, path):
    return {'artifact_id': artifact_id, 'task_id': task_id, 'path': path, 'kind': 'report', 'note': 'n',
            'registered_at': '2026-01-10T10:04:00+00:00',
            'provenance': {'method': 'verified_process_lineage', 'task_id': task_id},
            'registered_observation': {'device': 1, 'inode': 2, 'size': 3, 'mtime_ns': 4}}


ART1, ART2 = '11111111-2222-4333-8444-555555555555', '11111111-2222-4333-8444-666666666666'


def write_manifest(ctx, task_id, items, **header):
    document = {'product': 'harflow', 'format': 'harflow.task.artifacts', 'v': 1, 'product_version': seed.PRODUCT_VERSION,
                'task_id': task_id, 'items': items}
    document.update(header)
    path = ctx.tasks / f'{task_id}.artifacts.json'
    path.write_text(json.dumps(document))
    return path


@case('task-artifacts.pages_and_errors')
def _(ctx):
    ctx.seed_reads()
    empty = success(ctx, ctx.ctl('task-artifacts', A, '--json'))
    ctx.eq({k: empty[k] for k in ('task_id', 'items', 'count', 'has_more', 'next_cursor', 'state')},
           {'task_id': A, 'items': [], 'count': 0, 'has_more': False, 'next_cursor': None, 'state': 'available'},
           'a task with no manifest has an empty available set [contract]')
    manifest = write_manifest(ctx, A, [artifact_item(A, ART1, '/workspace/repo/out/a.txt'), artifact_item(A, ART2, '/workspace/repo/out/b.txt')])
    ctx.rebase()
    first = success(ctx, ctx.ctl('task-artifacts', A, '--json', '--limit', '1'))
    ctx.eq((first['count'], first['has_more'], [i['artifact_id'] for i in first['items']]), (2, True, [ART1]), 'first artifact page')
    item = first['items'][0]
    subset(ctx, item, {'path': '/workspace/repo/out/a.txt', 'kind': 'report', 'note': 'n', 'task_id': A,
                       'registered_observation': {'device': 1, 'inode': 2, 'size': 3, 'mtime_ns': 4}}, 'artifact item [contract]')
    ctx.eq(item['availability']['state'], 'deleted', 'a reference whose target is absent is deleted [contract]')
    ctx.check(isinstance(first['next_cursor'], str) and first['next_cursor'], 'continuing page carries a cursor')
    second = success(ctx, ctx.ctl('task-artifacts', A, '--json', '--limit', '1', '--cursor', first['next_cursor']))
    ctx.eq(([i['artifact_id'] for i in second['items']], second['has_more'], second['next_cursor']), ([ART2], False, None), 'second artifact page')
    error(ctx, ctx.ctl('task-artifacts', A, '--json', '--cursor', 'zzz'), 'artifact_error')
    error(ctx, ctx.ctl('task-artifacts', A, '--json', '--limit', '0'), 'artifact_error')
    error(ctx, ctx.ctl('task-artifacts', A, '--json', '--limit', '51'), 'artifact_error')
    error(ctx, ctx.ctl('task-artifacts', MISSING, '--json'), 'artifact_error', label='unknown task')
    write_manifest(ctx, A, [], format='harflow.other')
    ctx.rebase()
    broken = success(ctx, ctx.ctl('task-artifacts', A, '--json'))
    ctx.eq((broken['state'], broken['items'], broken['count']), ('unavailable', [], 0), 'a foreign manifest header reports unavailable [contract]')
    ctx.check(manifest.exists(), 'self-check: manifest stayed in place')


# --- recovery-activity --------------------------------------------------------------------------------

@case('recovery-activity.requires_capture_loss')
def _(ctx):
    ctx.seed_reads()
    res = ctx.ctl('recovery-activity', A, '--json')
    ctx.eq(res.rc, 0, 'recovery-activity exit status')
    doc = res.doc or {}
    ctx.eq(doc.get('task_id'), A, 'task_id')
    ctx.eq(doc.get('recovery_activity'), {'source': 'backend_transcript', 'display_only': True, 'events': [], 'next_cursor': None,
                                          'has_more': False, 'reset': False,
                                          'notices': ['Transcript recovery requires confirmed capture loss']},
           'without confirmed capture loss the page is empty and says why [baseline]')
    doc = success(ctx, ctx.ctl('recovery-activity', 'aaaaaaaa', '--json'))
    ctx.eq(doc.get('resolved_from'), {'input': 'aaaaaaaa', 'task_id': A}, 'prefix echo [contract]')
    error(ctx, ctx.ctl('recovery-activity', MISSING, '--json'), 'unknown_task')
    error(ctx, ctx.ctl('recovery-activity', 'bad/id', '--json'), 'invalid_task_id')


# --- interaction-events -------------------------------------------------------------------------------

IE = ('interaction-events.v1',)


@case('interaction-events.feed_and_paging', CORE + IE)
def _(ctx):
    ctx.seed_reads()
    result = success(ctx, ctx.ctl('interaction-events', '--json'))
    finished = {A: ('completed', '2026-01-10T10:05:00+00:00'), B: ('failed', '2026-01-11T10:03:00+00:00'),
                C: ('completed', '2026-01-12T10:02:00+00:00'), D: ('completed', '2026-01-13T10:02:00+00:00'),
                E: ('completed', '2026-01-14T10:02:00+00:00')}
    ctx.eq([e['event_id'] for e in result['events']], [f'task:{t}:{s}' for t, (s, _) in finished.items()],
           'one terminal task event per observed settled task, oldest first [contract]')
    ctx.eq((result['cycle_complete'], result['next_cursor'], result['history_incomplete'], result['gaps']), (True, None, False, []),
           'a complete cycle has no cursor and no gaps [contract]')
    for row in result['events']:
        task_id = row['task_id']
        state, stamp = finished[task_id]
        subset(ctx, row, {'interaction_id': task_id, 'kind': 'task', 'state': state, 'occurred_at': epoch(stamp),
                          'workflow_run_id': None, 'source_workflow_run_id': None}, f'event {task_id[:8]} [contract]')
        ctx.check(isinstance(row.get('title'), str) and row['title'], f'event {task_id[:8]} has a display title [contract]')
    titles = {row['task_id']: row['title'] for row in result['events']}
    ctx.eq((titles[A], titles[B], titles[C]), ('First task', 'Second task', 'Third task'), 'titles come from the task record [contract]')
    collected, cursor, pages = [], None, 0
    while True:
        args = ['interaction-events', '--json', '--limit', '2'] + (['--cursor', cursor] if cursor else [])
        page = success(ctx, ctx.ctl(*args))
        collected += [e['event_id'] for e in page['events']]
        pages += 1
        ctx.check(len(page['events']) <= 2, 'a page respects --limit [contract]')
        if page['cycle_complete']:
            ctx.eq(page['next_cursor'], None, 'the closing page has no cursor [contract]')
            break
        cursor = page['next_cursor']
        ctx.check(isinstance(cursor, str) and cursor, 'an unfinished cycle carries a cursor [contract]')
        ctx.check(pages < 10, 'cycle terminates')
    ctx.eq(collected, [e['event_id'] for e in result['events']], 'paged cycle equals the unpaged feed, without duplicates [contract]')


@case('interaction-events.errors', CORE + IE)
def _(ctx):
    ctx.seed_reads()
    error(ctx, ctx.ctl('interaction-events', '--json', '--limit', '0'), 'interaction_events_error')
    error(ctx, ctx.ctl('interaction-events', '--json', '--limit', '101'), 'interaction_events_error')
    error(ctx, ctx.ctl('interaction-events', '--json', '--cursor', 'zzz'), 'interaction_events_error')
    ctx.eq(len(success(ctx, ctx.ctl('interaction-events', '--json', '--limit', '100'))['events']), 5, '100 is the inclusive upper bound [contract]')


# --- statistics-page ----------------------------------------------------------------------------------

ST = ('statistics-page.v1',)


def stats_ready(ctx, *args):
    for _ in range(40):
        res = ctx.ctl(*args)
        result = success(ctx, res)
        if result.get('state') != 'preparing':
            return res
        time.sleep(0.25)
    ctx.check(False, 'statistics-page leaves preparing within the bounded retries')
    return res


@case('statistics-page.window_and_paging', CORE + ST)
def _(ctx):
    ctx.seed_reads()
    res = stats_ready(ctx, 'statistics-page', *WINDOW, '--json')
    result = res.result
    ctx.eq((result['state'], result['history_complete'], result['has_more'], result['next_cursor']), ('ready', True, False, None),
           'ready, complete, single page [contract]')
    rows = {row['task_id']: row for row in result['items']}
    ctx.eq(set(rows), ALL_IDS, 'every finished standalone execution starting in the window is one item [contract]')
    elapsed = {A: 300.0, B: 180.0, C: 120.0, D: 120.0, E: 120.0}
    for task_id, row in rows.items():
        subset(ctx, row, {'kind': 'execution', 'elapsed_seconds': elapsed[task_id], 'requested_harness': 'claude', 'workflow_run_id': None},
               f'statistics item {task_id[:8]} [contract]')
    subset(ctx, rows[A], {'status': 'completed', 'title': 'First task', 'requested_model': 'model-a', 'execution_kind': 'initial',
                          'started_at': epoch('2026-01-10T10:00:00+00:00'), 'finished_at': epoch('2026-01-10T10:05:00+00:00')}, 'statistics[A]')
    subset(ctx, rows[B], {'status': 'failed'}, 'statistics[B]')
    subset(ctx, rows[C], {'execution_kind': 'continuation', 'continuation_of_task_id': A, 'chain_root_task_id': A, 'requested_model': None},
           'statistics[C] unknown requested model stays null [contract]')
    subset(ctx, result['accounting_coverage'], {'eligible_executions': 5}, 'accounting_coverage [contract]')
    subset(ctx, result['cost_coverage'], {'eligible_executions': 5}, 'cost_coverage [contract]')
    ctx.check(isinstance(result.get('generation'), str) and result['generation'], 'generation is present [contract]')
    paged, cursor, generations = [], None, set()
    for _ in range(10):
        args = ['statistics-page', *WINDOW, '--limit', '2', '--json'] + (['--cursor', cursor] if cursor else [])
        page = stats_ready(ctx, *args).result
        paged += [row['task_id'] for row in page['items']]
        generations.add(page['generation'])
        ctx.check(len(page['items']) <= 2, 'a page respects --limit [contract]')
        if not page['has_more']:
            break
        cursor = page['next_cursor']
        ctx.check(isinstance(cursor, str) and cursor, 'a continuing page carries a cursor [contract]')
    ctx.eq((sorted(paged), len(paged), len(generations)), (sorted(ALL_IDS), 5, 1), 'pages of one generation are disjoint and complete [contract]')
    narrow = stats_ready(ctx, 'statistics-page', '--since', '2026-01-12T00:00:00+00:00', '--before', '2026-01-13T00:00:00+00:00', '--json').result
    ctx.eq([row['task_id'] for row in narrow['items']], [C], 'since is inclusive and before exclusive, filtering by start time [contract]')
    edge = stats_ready(ctx, 'statistics-page', '--since', '2026-01-12T10:00:00+00:00', '--before', '2026-01-12T10:00:01+00:00', '--json').result
    ctx.eq([row['task_id'] for row in edge['items']], [C], 'a start exactly at since is included [contract]')
    edge = stats_ready(ctx, 'statistics-page', '--since', '2026-01-12T09:00:00+00:00', '--before', '2026-01-12T10:00:00+00:00', '--json').result
    ctx.eq(edge['items'], [], 'a start exactly at before is excluded [contract]')
    other_zone = stats_ready(ctx, 'statistics-page', '--since', '2026-01-12T17:00:00+07:00', '--before', '2026-01-12T17:00:01+07:00', '--json').result
    ctx.eq([row['task_id'] for row in other_zone['items']], [C], 'time zone offsets are honoured [contract]')
    empty = stats_ready(ctx, 'statistics-page', '--since', '2027-01-01T00:00:00+00:00', '--before', '2027-02-01T00:00:00+00:00', '--json').result
    ctx.eq((empty['items'], empty['has_more'], empty['state'], empty['history_complete']), ([], False, 'ready', True), 'an empty window is ready and empty [contract]')
    subset(ctx, empty['accounting_coverage'], {'eligible_executions': 0}, 'empty coverage')


@case('statistics-page.errors', CORE + ST)
def _(ctx):
    ctx.seed_reads()
    for since, before in (('2026-01-01T00:00:00', '2026-02-01T00:00:00+00:00'), ('2026-01-01T00:00:00+00:00', '2026-02-01T00:00:00'),
                          ('2026-02-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00'), ('2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00'),
                          ('nonsense', '2026-02-01T00:00:00+00:00')):
        error(ctx, ctx.ctl('statistics-page', '--since', since, '--before', before, '--json'), 'invalid_params',
              label=f'statistics-page {since}..{before}')
    for limit in ('0', '101'):
        error(ctx, ctx.ctl('statistics-page', *WINDOW, '--limit', limit, '--json'), 'invalid_params', label=f'statistics-page limit {limit}')
    error(ctx, ctx.ctl('statistics-page', *WINDOW, '--cursor', 'bogus', '--json'), 'invalid_cursor')
    ctx.eq(ctx.ctl('statistics-page', '--json').rc, 2, 'missing required bounds is a usage error [contract]')


# --- scoped reads and caller rules --------------------------------------------------------------------

DIRECT_ONLY = [
    ('task-events', ['task-events', A, '--json'], 'caller_refusal'),
    ('task-events (unknown id)', ['task-events', MISSING, '--json'], 'caller_refusal'),
    ('task-prompt', ['task-prompt', A, '--json'], 'caller_refusal'),
    ('statistics-page', ['statistics-page', *WINDOW, '--json'], 'caller_refusal'),
    ('successor discovery', ['task-list-page', '--json', '--predecessor-task-id', A], 'caller_refusal'),
    ('interaction-events', ['interaction-events', '--json'], 'unauthorized'),
]
SCOPE_CAPS = CORE + EV + PR + ST + IE + ('task-successors.v1',)


@case('scope.agent_caller_by_ancestry', SCOPE_CAPS)
def _(ctx):
    ctx.seed_reads()
    agent_caller(ctx, CALLER)
    for label, args, kind in DIRECT_ONLY:
        res = ctx.ctl(*args)
        if kind == 'caller_refusal':
            err = error(ctx, res, 'agent_caller', label=label)
            ctx.check(CALLER in err.get('message', '') and 'ancestry' in err.get('message', ''),
                      f'{label}: the refusal names the calling task and the detection method [baseline]')
        else:
            error(ctx, res, 'unauthorized', label=label)
        ctx.check(A not in json.dumps(res.doc) and MISSING not in json.dumps(res.doc),
                  f'{label}: a refused caller learns nothing about which ids exist [contract]')
    unknown = ctx.ctl('task-events', MISSING, '--json')
    known = ctx.ctl('task-events', A, '--json')
    ctx.eq((unknown.doc or {}).get('error'), (known.doc or {}).get('error'),
           'authority precedes the task lookup: known and unknown ids refuse identically [contract]')
    # Reads that managed workflow callers are scoped on stay available to a standalone task caller.
    success(ctx, ctx.ctl('status', A, '--json'))
    success(ctx, ctx.ctl('list', '--json'))
    success(ctx, ctx.ctl('recovery-activity', A, '--json'))
    success(ctx, ctx.ctl('status-batch', '--task-ids', A, '--json'))
    own = success(ctx, ctx.ctl('task-artifacts', CALLER, '--json'))
    ctx.eq((own['task_id'], own['state'], own['count']), (CALLER, 'available', 0), 'a task caller reads its own artifacts [contract]')
    err = error(ctx, ctx.ctl('task-artifacts', A, '--json'), 'artifact_error')
    ctx.eq(err.get('message'), 'Task callers may read only their own artifacts', 'a task caller may not read another task\'s artifacts [baseline]')
    page = page_ready(ctx, 'task-list-page', '--json')
    ctx.eq(set(ids(page.result['items'])), ALL_IDS | {CALLER}, 'a standalone task caller sees the unscoped page [baseline]')


@case('scope.environment_markers', SCOPE_CAPS)
def _(ctx):
    ctx.seed_reads()
    for env in ({'HARFLOW_TASK_ID': MISSING}, {'HARFLOW_TASK_ID': A}, {'PB_TASK_ID': MISSING}, {'HARFLOW_DEPTH': '1'},
                {'HARFLOW_DEPTH': 'x'}, {'PB_DEPTH': '2'}):
        label = ','.join(f'{k}={v[:8]}' for k, v in env.items())
        for name, args, kind in DIRECT_ONLY:
            res = ctx.ctl(*args, env=env)
            if kind == 'caller_refusal':
                err = error(ctx, res, 'agent_caller', label=f'{name} [{label}]')
                ctx.check('HARFLOW_TASK_ID' in err.get('message', ''), f'{name} [{label}]: the refusal names the worker variables [baseline]')
            else:
                error(ctx, res, 'unauthorized', label=f'{name} [{label}]')


@case('scope.unverifiable_task_identity', SCOPE_CAPS)
def _(ctx):
    ctx.seed_reads()
    env = {'HARFLOW_TASK_ID': MISSING}
    for args in (['status', A, '--json'], ['list', '--json'], ['status-batch', '--task-ids', A, '--json'], ['recovery-activity', A, '--json']):
        error(ctx, ctx.ctl(*args, env=env), 'workflow_read_refused', label=' '.join(args[:1]) + ' with an unverifiable HARFLOW_TASK_ID')
    error(ctx, ctx.ctl('task-artifacts', A, '--json', env=env), 'artifact_error', label='task-artifacts with an unverifiable HARFLOW_TASK_ID')
    for args in (['status', A, '--json'], ['list', '--json']):
        success(ctx, ctx.ctl(*args, env={'HARFLOW_TASK_ID': None}), 'unset marker reads normally')
    for env in ({'HARFLOW_DEPTH': '1'}, {'PB_TASK_ID': MISSING}):
        label = ','.join(env)
        for args in (['status', A, '--json'], ['list', '--json']):
            success(ctx, ctx.ctl(*args, env=env), f'{args[0]} with only {label} set [baseline]')
    live = '34343434-3434-4434-8434-343434343434'
    agent_caller(ctx, live)
    holder = ctx.ctl('status', A, '--json', env={'HARFLOW_TASK_ID': MISSING})
    ctx.eq(holder.rc, 0, 'a marker naming an unknown task is overridden by a confirmed ancestry caller [baseline]')


@case('scope.incompatible_metadata_fails_closed', SCOPE_CAPS)
def _(ctx):
    ctx.seed_reads()
    seed._write(ctx.tasks / f'{seed.BROKEN}.meta.json',
                json.dumps({'task_id': seed.BROKEN, 'product': 'other', 'format': 'x', 'v': 1, 'product_version': seed.PRODUCT_VERSION}))
    ctx.rebase()
    for args in (['list', '--json'], ['status', A, '--json'], ['status-batch', '--task-ids', A, '--json'], ['recovery-activity', A, '--json']):
        error(ctx, ctx.ctl(*args), 'workflow_read_refused', label=args[0] + ' with unverifiable history')
    for name, args, kind in DIRECT_ONLY:
        if kind == 'caller_refusal':
            error(ctx, ctx.ctl(*args), 'caller_undecidable', label=name + ' with unverifiable history')
        else:
            error(ctx, ctx.ctl(*args), 'unauthorized', label=name + ' with unverifiable history')
    error(ctx, ctx.ctl('task-artifacts', A, '--json'), 'artifact_error', label='task-artifacts with unverifiable history')
    page = success(ctx, ctx.ctl('task-list-page', '--json'))
    ctx.eq((page.get('items'), page.get('authority_incomplete'), page.get('history_incomplete'), page.get('has_more'), page.get('next_cursor')),
           ([], True, True, False, None), 'the page read withholds all items when authority is incomplete [contract]')
    ctx.check(isinstance(page.get('note'), str) and page['note'], 'the withheld page carries a safe note [contract]')
    ctx.check(set(page.get('catalog_state', {})) <= {'status', 'source', 'reason'}, 'withheld page exposes only the allowlisted catalog state [contract]')


# --- cursor binding -----------------------------------------------------------------------------------

@case('cursors.binding', CORE + EV + ST)
def _(ctx):
    ctx.seed_reads()
    other = ctx.folder / 'state-other'
    seed.seed_reads(other)
    elsewhere = {'HARFLOW_STATE_ROOT': str(other)}
    cursor = page_ready(ctx, 'task-list-page', '--json', '--limit', '2').result['next_cursor']
    page_ready(ctx, 'task-list-page', '--json', env=elsewhere)
    for label, extra, env in (('another state root', [], elsewhere), ('a different active filter', ['--active-only'], None),
                              ('a different session filter', ['--session-id', 'x'], None),
                              ('a different predecessor scope', ['--predecessor-task-id', A], None)):
        error(ctx, ctx.ctl('task-list-page', '--json', '--limit', '2', '--cursor', cursor, *extra, env=env), 'invalid_params',
              label=f'task-list-page cursor under {label} [baseline]')
    ctx.eq(ids(success(ctx, ctx.ctl('task-list-page', '--json', '--limit', '3', '--cursor', cursor))['items']), [C, B, A],
           'a page cursor survives a change of limit [baseline]')
    token = stats_ready(ctx, 'statistics-page', *WINDOW, '--limit', '2', '--json').result['next_cursor']
    error(ctx, ctx.ctl('statistics-page', *WINDOW, '--limit', '2', '--json', '--cursor', token, env=elsewhere), 'invalid_cursor',
          label='statistics cursor under another state root [contract]')
    error(ctx, ctx.ctl('statistics-page', '--since', '2026-01-02T00:00:00+00:00', '--before', '2026-02-01T00:00:00+00:00',
                       '--limit', '2', '--json', '--cursor', token), 'invalid_cursor', label='statistics cursor under other bounds [contract]')
    seed.write_task(ctx.tasks, '12345678-0000-4000-8000-0000000000aa', started_at='2026-01-15T10:00:00+00:00',
                    finished_at='2026-01-15T10:01:00+00:00')
    ctx.rebase()
    error(ctx, ctx.ctl('statistics-page', *WINDOW, '--limit', '2', '--json', '--cursor', token), 'snapshot_expired',
          label='statistics cursor after a relevant mutation [contract]')
    event_cursor = success(ctx, ctx.ctl('task-events', A, '--json', '--from', 'oldest', '--limit', '2'))['newer_cursor']
    error(ctx, ctx.ctl('task-events', A, '--json', '--cursor', event_cursor, env=elsewhere), 'event_cursor_expired',
          label='event cursor under another state root [contract]')


# --- reads against a missing root ---------------------------------------------------------------------

@case('reads.missing_root', SCOPE_CAPS)
def _(ctx):
    import shutil
    expectations = [
        (['list', '--json'], 'tasks', []),
        (['status', A, '--json'], 'unknown_task', None),
        (['status-batch', '--task-ids', A, '--json'], 'status-batch', None),
        (['task-list-page', '--json'], 'items', []),
        (['task-list-page', '--json', '--predecessor-task-id', A], 'unknown_task', None),
        (['task-events', A, '--json'], 'unknown_task', None),
        (['task-prompt', A, '--json'], 'unknown_task', None),
        (['task-artifacts', A, '--json'], 'artifact_error', None),
        (['recovery-activity', A, '--json'], 'unknown_task', None),
        (['interaction-events', '--json'], 'events', []),
        (['statistics-page', *WINDOW, '--json'], 'items', []),
    ]
    for args, key, empty in expectations:
        shutil.rmtree(ctx.root, ignore_errors=True)
        label = ' '.join(args[:1]) + (' successors' if '--predecessor-task-id' in args else '') + ' on a missing root'
        res = ctx.ctl(*args)
        if empty is not None:
            result = success(ctx, res, label)
            ctx.eq(result.get(key), empty, f'{label}: answers as empty [contract]')
        elif key == 'status-batch':
            doc = success(ctx, res, label)
            ctx.eq((doc['tasks'], doc['errors'][A]['code']), ([], 'unknown_task'), f'{label}: every id is unknown [contract]')
        else:
            error(ctx, res, key, label=label)
        marker = ctx.root / '.harflow-store.json'
        ctx.check(marker.is_file() and json.loads(marker.read_text()).get('format') == 'harflow.store',
                  f'{label}: an ordinary read initializes the selected root [contract]')
    shutil.rmtree(ctx.root, ignore_errors=True)
    ctx.baseline = None
