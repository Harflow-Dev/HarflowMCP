"""E1 read cases. Expectations are literals derived from `world.py` and the published contract.

Each case runs in a fresh isolated state root against the public command line only, and creates or
damages state exclusively through the fixture port (`port.py`). Checks tagged `[contract]` follow
`docs/contracts.md`; every other check is `[baseline]`: it pins observed Python 0.0.11 behaviour that the
contracts do not spell out, so an engine that differs must record a contract delta (C1) rather than
silently diverge. `[baseline, pending U2]`, `[baseline, U4]` and `[proposed]` mark checks whose
status the owner has yet to decide (see `docs/decisions/C1-caller-and-scoped-reads.md`).
"""
import json
import os
import subprocess
import time
from datetime import datetime

from . import harness
from .world import A, B, C, D, E, MISSING, BROKEN, LONG_PROMPT, PREVIEW_CHARS, WORLD_READS

CASES = {}
CORE = ('ctl.v1',)
MAX_PAGES = 12


def case(name, capabilities=CORE, tag='baseline'):
    def wrap(function):
        CASES[name] = (capabilities, function, tag)
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


CALLER = '12121212-1212-4212-8212-121212121212'
WINDOW = ['--since', '2026-01-01T00:00:00+00:00', '--before', '2026-02-01T00:00:00+00:00']
ALL_IDS = {A, B, C, D, E}


def own_marker():
    """A substring of this process's own command line, for identities that carry markers."""
    out = subprocess.run(['ps', '-o', 'command=', '-p', str(os.getpid())], capture_output=True, text=True,
                         env={**os.environ, 'LC_ALL': 'C'}).stdout.strip()
    token = out.split()[0] if out else ''
    return token if token and token in out else None


def agent_caller(ctx, task_id, identity='alive', status='running'):
    """Register the test process itself as a live task's process: the engine, being its descendant,
    is run by a verified agent caller (detected by ancestry). Returns whether the adapter supports it."""
    marker = own_marker() if identity in ('alive_with_markers', 'legacy_markers_seen') else None
    result = ctx.fx.add_live_caller(task_id, os.getpid(), identity=identity, status=status, marker=marker) or {}
    ctx.rebase()
    return bool(result.get('supported', True))


def pending_shape(ctx, result, label):
    ctx.eq(result.get('items'), [], f'{label}: preparing page has no items [contract]')
    ctx.eq(result.get('related_headers'), [], f'{label}: preparing page has no related headers [contract]')
    ctx.eq(result.get('next_cursor'), None, f'{label}: preparing page has no cursor [contract]')
    ctx.eq(result.get('has_more'), False, f'{label}: preparing page has_more [contract]')
    ctx.eq(result.get('counts_complete'), False, f'{label}: preparing page counts_complete [contract]')
    state = result.get('catalog_state')
    ctx.check(isinstance(state, dict) and set(state) <= {'status', 'source', 'reason'},
              f'{label}: catalog_state exposes only status, source, reason [contract]')


def settled_page(ctx, *args, env=None):
    """Re-issue a page read while it reports preparation (the contract tells clients to retry), under the
    harness bootstrap policy. Returns the first settled response, which may be an error."""
    res = None
    for _ in range(harness.BOOTSTRAP_ATTEMPTS):
        res = ctx.ctl(*args, env=env)
        result = res.result if res.rc == 0 and res.doc else None
        if result is None or result.get('bootstrap_pending') is not True:
            return res
        pending_shape(ctx, result, 'task-list-page')
        time.sleep(harness.BOOTSTRAP_INTERVAL_SECONDS)
    ctx.check(False, 'task-list-page leaves bootstrap_pending within the harness bootstrap budget', tag='harness')
    return res


def page_ready(ctx, *args, env=None):
    res = settled_page(ctx, *args, env=env)
    success(ctx, res)
    return res


def ids(items):
    return [row['task_id'] for row in items]


# --- list ---------------------------------------------------------------------------------------------
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
    ctx.fx.add_task({'task_id': recent, 'started_at': stamp, 'finished_at': stamp, 'prompt': 'recent', 'status': 'completed', 'exit_code': 0})
    ctx.rebase()
    doc = success(ctx, ctx.ctl('list', '--json', '--since', '7d'))
    ctx.eq([row['task_id'] for row in doc['tasks']], [recent], 'only a task started within the window is listed [contract]')
    doc = success(ctx, ctx.ctl('list', '--json', '--since', '10000d'))
    ctx.eq({row['task_id'] for row in doc['tasks']}, ALL_IDS | {recent}, 'a wide window lists every task')
    error(ctx, ctx.ctl('list', '--json', '--since', 'bogus'), 'usage', rc=2)


@case('list.missing_root_initializes')
def _(ctx):
    ctx.eq(ctx.fx.root_state(), 'missing', 'precondition: no state root')
    doc = success(ctx, ctx.ctl('list', '--json'))
    ctx.eq(doc['tasks'], [], 'an empty store lists no tasks')
    ctx.eq(ctx.fx.root_state(), 'initialized', 'an ordinary read initializes a missing root [contract, pending U10]')


@case('list.foreign_root_refused')
def _(ctx):
    ctx.fx.make_foreign_root()
    ctx.rebase()
    res = ctx.ctl('list', '--json')
    err = error(ctx, res, 'state_incompatible')
    subset(ctx, err.get('data') or {}, {'reason': 'unmarked_nonempty', 'state_root': str(ctx.root)}, 'error.data [contract]')
    ctx.eq(ctx.fx.root_state(), 'other', 'a refused root is left untouched [contract]')


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
        ctx.fx.add_task({'task_id': name, 'started_at': '2026-01-10T10:00:00+00:00', 'finished_at': '2026-01-10T10:01:00+00:00',
                         'status': 'completed', 'exit_code': 0, 'prompt': 'p'})
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
    ctx.check('resolved_from' not in rows[2], 'exact row has no resolved_from [contract]')
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
    ctx.eq(ids(result['items']), [E, D], 'first page is the two newest tasks, newest first')
    ctx.eq(result['has_more'], True, 'first page has_more')
    ctx.eq(result.get('counts_complete'), True, 'ready page reports counts_complete')
    ctx.eq(result.get('total_active_count'), 0, 'no active tasks are counted')
    seen = ids(result['items'])
    cursor = result['next_cursor']
    ctx.check(isinstance(cursor, str) and cursor, 'a continuing page carries an opaque next_cursor [contract]')
    for expected, more in (([C, B], True), ([A], False)):
        res = ctx.ctl('task-list-page', '--json', '--limit', '2', '--cursor', cursor)
        page = success(ctx, res)
        ctx.eq(ids(page['items']), expected, 'continuation page items')
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
    for _ in range(MAX_PAGES):
        if not page['has_newer']:
            break
        page = success(ctx, ctx.ctl('task-events', A, '--json', '--cursor', page['newer_cursor'], '--limit', '2'))
        collected += seqs(page)
        spans.append(page['span'])
    else:
        ctx.check(False, f'forward paging reaches the end of the log within {MAX_PAGES} pages [contract]')
        return
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
    ctx.fx.add_task({'task_id': gappy, 'started_at': '2026-01-10T10:00:00+00:00', 'finished_at': '2026-01-10T10:05:00+00:00',
                     'status': 'completed', 'exit_code': 0, 'prompt': 'p'})
    ctx.fx.append_event(gappy, 0, 'task_started', '2026-01-10T10:00:00+00:00', {})
    ctx.fx.append_event(gappy, 1, 'assistant_text', '2026-01-10T10:00:01+00:00', {'text': 'a'})
    ctx.fx.append_malformed_line(gappy)
    ctx.fx.append_event(gappy, 5, 'assistant_text', '2026-01-10T10:00:02+00:00', {'text': 'b'})
    ctx.fx.append_event(gappy, 6, 'task_finished', '2026-01-10T10:00:03+00:00', {'status': 'completed', 'exit_code': 0})
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
    page = success(ctx, ctx.ctl('task-events', A, '--json', '--from', 'oldest', '--limit', '4'))
    cursor = page['newer_cursor']
    ctx.fx.append_event(A, 8, 'assistant_text', '2026-01-10T10:06:00+00:00', {'text': 'late'})
    ctx.fx.append_partial_line(A)
    ctx.rebase()
    after = success(ctx, ctx.ctl('task-events', A, '--json', '--cursor', cursor, '--limit', '50'))
    ctx.eq(seqs(after), [4, 5, 6, 7, 8], 'an append does not expire the cursor and a partial trailing line is held back [contract]')
    ctx.eq(after['has_newer'], False, 'a partial trailing line never sets has_newer [contract]')
    ctx.eq(after['log']['state'], 'incomplete', 'a log whose last complete line is not task_finished is not final [contract]')
    ctx.fx.truncate_log(A, 1)
    ctx.rebase()
    err = error(ctx, ctx.ctl('task-events', A, '--json', '--cursor', cursor), 'event_cursor_expired')
    ctx.eq(err.get('data'), {'reload': 'newest'}, 'truncation expires the cursor with reload hint [contract]')
    ctx.fx.remove_log(A)
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
    tampered = '12345678-0000-4000-8000-0000000000f1'
    foreign = '12345678-0000-4000-8000-0000000000f2'
    for task_id, store in ((tampered, 'sidecar_verified'), (foreign, 'sidecar_foreign_name')):
        ctx.fx.add_task({'task_id': task_id, 'started_at': '2026-01-10T10:00:00+00:00', 'finished_at': '2026-01-10T10:01:00+00:00',
                         'status': 'completed', 'exit_code': 0, 'prompt': LONG_PROMPT, 'prompt_store': store})
    ctx.fx.tamper_prompt(tampered)
    ctx.rebase()
    result = success(ctx, ctx.ctl('task-prompt', A, '--json'))
    ctx.eq((result['task_id'], result['title']), (A, 'First task'), 'task-prompt identity')
    ctx.eq(result['prompt'], {'state': 'available', 'text': 'Summarize the repo.', 'returned_bytes': 19, 'total_bytes': 19,
                              'truncated': False, 'complete': True, 'integrity': 'not_applicable'}, 'inline prompt [contract]')
    full = LONG_PROMPT
    result = success(ctx, ctx.ctl('task-prompt', D, '--json'))
    ctx.eq(result['prompt'], {'state': 'available', 'text': full, 'returned_bytes': len(full.encode()), 'total_bytes': len(full.encode()),
                              'truncated': False, 'complete': True, 'integrity': 'verified'}, 'sidecar prompt is verbatim and verified [contract]')
    cut = success(ctx, ctx.ctl('task-prompt', D, '--json', '--max-bytes', '11'))['prompt']
    head = full.encode()[:11].decode('utf-8', 'ignore')
    ctx.eq(cut, {'state': 'available', 'text': head, 'returned_bytes': len(head.encode()), 'total_bytes': len(full.encode()),
                 'truncated': True, 'complete': False, 'integrity': 'verified'}, 'a cut drops only an incomplete trailing character [contract]')
    ctx.eq(len(head.encode()), 10, 'self-check: the multibyte character straddles the bound [harness]')
    one = success(ctx, ctx.ctl('task-prompt', D, '--json', '--max-bytes', '1'))['prompt']
    ctx.eq((one['text'], one['returned_bytes'], one['truncated']), ('l', 1, True), '--max-bytes 1 [contract]')
    preview = success(ctx, ctx.ctl('task-prompt', E, '--json'))['prompt']
    ctx.eq((preview['state'], preview['text'], preview['complete'], preview['total_bytes'], preview['integrity']),
           ('preview_only', full[:PREVIEW_CHARS], False, None, 'not_applicable'), 'missing sidecar gives the non-authoritative preview [contract]')
    failed = success(ctx, ctx.ctl('task-prompt', tampered, '--json'))['prompt']
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

ART1, ART2 = '11111111-2222-4333-8444-555555555555', '11111111-2222-4333-8444-666666666666'
PRESENT = '13131313-1313-4313-8313-131313131313'


@case('task-artifacts.pages_and_errors')
def _(ctx):
    ctx.seed_reads()
    empty = success(ctx, ctx.ctl('task-artifacts', A, '--json'))
    ctx.eq({k: empty[k] for k in ('task_id', 'items', 'count', 'has_more', 'next_cursor', 'state')},
           {'task_id': A, 'items': [], 'count': 0, 'has_more': False, 'next_cursor': None, 'state': 'available'},
           'a task with no manifest has an empty available set [contract]')
    ctx.fx.write_artifacts(A, [{'artifact_id': ART1, 'name': 'a.txt'}, {'artifact_id': ART2, 'name': 'b.txt'}])
    ctx.rebase()
    first = success(ctx, ctx.ctl('task-artifacts', A, '--json', '--limit', '1'))
    ctx.eq((first['count'], first['has_more'], [i['artifact_id'] for i in first['items']]), (2, True, [ART1]), 'first artifact page')
    item = first['items'][0]
    subset(ctx, item, {'path': '/workspace/repo/out/a.txt', 'kind': 'report', 'note': 'n', 'task_id': A,
                       'registered_observation': {'device': 1, 'inode': 2, 'size': 3, 'mtime_ns': 4}}, 'artifact item [contract]')
    ctx.eq(item['availability']['state'], 'deleted', 'a reference whose target is absent is deleted [contract]')
    ctx.check(isinstance(first['next_cursor'], str) and first['next_cursor'], 'continuing page carries a cursor [contract]')
    second = success(ctx, ctx.ctl('task-artifacts', A, '--json', '--limit', '1', '--cursor', first['next_cursor']))
    ctx.eq(([i['artifact_id'] for i in second['items']], second['has_more'], second['next_cursor']), ([ART2], False, None), 'second artifact page')
    error(ctx, ctx.ctl('task-artifacts', A, '--json', '--cursor', 'zzz'), 'artifact_error')
    ctx.fx.write_artifacts(A, [{'artifact_id': ART1, 'name': 'a.txt'}])
    ctx.rebase()
    error(ctx, ctx.ctl('task-artifacts', A, '--json', '--limit', '1', '--cursor', first['next_cursor']), 'artifact_error',
          label='task-artifacts cursor after the manifest changed')
    error(ctx, ctx.ctl('task-artifacts', A, '--json', '--limit', '0'), 'artifact_error')
    error(ctx, ctx.ctl('task-artifacts', A, '--json', '--limit', '51'), 'artifact_error')
    error(ctx, ctx.ctl('task-artifacts', MISSING, '--json'), 'artifact_error', label='unknown task')
    ctx.fx.write_artifacts(A, [], header_valid=False)
    ctx.rebase()
    broken = success(ctx, ctx.ctl('task-artifacts', A, '--json'))
    ctx.eq((broken['state'], broken['items'], broken['count']), ('unavailable', [], 0), 'a foreign manifest header reports unavailable [contract]')


@case('task-artifacts.availability_and_prefixes')
def _(ctx):
    ctx.seed_reads()
    ctx.fx.add_task({'task_id': PRESENT, 'started_at': '2026-01-10T10:00:00+00:00', 'finished_at': '2026-01-10T10:01:00+00:00',
                     'status': 'completed', 'exit_code': 0, 'prompt': 'p', 'real_workspace': True})
    ctx.fx.write_artifacts(PRESENT, [{'artifact_id': ART1, 'name': 'a.txt', 'target': 'present'},
                                     {'artifact_id': ART2, 'name': 'b.txt', 'target': 'absent'}])
    ctx.rebase()
    states = lambda r: {i['artifact_id']: i['availability']['state'] for i in success(ctx, r)['items']}
    ctx.eq(states(ctx.ctl('task-artifacts', PRESENT, '--json')), {ART1: 'available', ART2: 'deleted'},
           'a present target with its registered observation is available, an absent one deleted [contract]')
    ctx.fx.change_artifact_target(PRESENT, 'a.txt')
    ctx.eq(states(ctx.ctl('task-artifacts', PRESENT, '--json')), {ART1: 'changed', ART2: 'deleted'},
           'a target whose stat differs from the registered observation is changed [contract]')
    result = success(ctx, ctx.ctl('task-artifacts', '13131313', '--json'))
    ctx.eq((result['task_id'], result.get('resolved_from')), (PRESENT, {'input': '13131313', 'task_id': PRESENT}), 'prefix resolves and is echoed [contract]')
    err = error(ctx, ctx.ctl('task-artifacts', 'nomatch0123', '--json'), 'unknown_task')
    ctx.eq((err.get('data') or {}).get('reason'), 'no_prefix_match', 'a non-canonical value of 8 or more characters that matches nothing [contract]')
    error(ctx, ctx.ctl('task-artifacts', 'zz', '--json'), 'artifact_error', label='a value shorter than 8 characters passes through [contract]')


# --- recovery-activity --------------------------------------------------------------------------------

@case('recovery-activity.requires_capture_loss')
def _(ctx):
    ctx.seed_reads()
    res = ctx.ctl('recovery-activity', A, '--json')
    ctx.eq(res.rc, 0, 'recovery-activity exit status')
    doc = res.doc or {}
    ctx.eq(doc.get('task_id'), A, 'task_id')
    page = doc.get('recovery_activity') or {}
    subset(ctx, page, {'source': 'backend_transcript', 'display_only': True, 'events': [], 'next_cursor': None,
                       'has_more': False, 'reset': False}, 'without confirmed capture loss the page is empty [baseline]')
    ctx.check(isinstance(page.get('notices'), list) and page['notices'] and all(isinstance(n, str) for n in page['notices']),
              'the empty page carries at least one explanatory notice')
    ctx.skip('recovery-activity with confirmed capture loss is not seedable through the fixture port; transcript paging is unverified')
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
    collected, cursor = [], None
    for _ in range(MAX_PAGES):
        args = ['interaction-events', '--json', '--limit', '2'] + (['--cursor', cursor] if cursor else [])
        page = success(ctx, ctx.ctl(*args))
        if 'events' not in page:
            ctx.check(False, 'every interaction page carries events [contract]')
            return
        collected += [e['event_id'] for e in page['events']]
        ctx.check(len(page['events']) <= 2, 'a page respects --limit [contract]')
        if page['cycle_complete']:
            ctx.eq(page['next_cursor'], None, 'the closing page has no cursor [contract]')
            break
        cursor = page['next_cursor']
        ctx.check(isinstance(cursor, str) and cursor, 'an unfinished cycle carries a cursor [contract]')
    else:
        ctx.check(False, f'the interaction cycle completes within {MAX_PAGES} pages [contract]')
        return
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
    res = None
    for _ in range(harness.BOOTSTRAP_ATTEMPTS):
        res = ctx.ctl(*args)
        result = success(ctx, res)
        if result.get('state') != 'preparing':
            return res
        time.sleep(harness.BOOTSTRAP_INTERVAL_SECONDS)
    ctx.check(False, 'statistics-page leaves preparing within the harness bootstrap budget', tag='harness')
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
    for _ in range(MAX_PAGES):
        args = ['statistics-page', *WINDOW, '--limit', '2', '--json'] + (['--cursor', cursor] if cursor else [])
        page = stats_ready(ctx, *args).result
        paged += [row['task_id'] for row in page['items']]
        generations.add(page['generation'])
        ctx.check(len(page['items']) <= 2, 'a page respects --limit [contract]')
        if not page['has_more']:
            break
        cursor = page['next_cursor']
        ctx.check(isinstance(cursor, str) and cursor, 'a continuing page carries a cursor [contract]')
    else:
        ctx.check(False, f'the statistics walk ends within {MAX_PAGES} pages [contract]')
        return
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


def refusal_code(kind, refusal):
    return refusal if kind == 'caller_refusal' else 'unauthorized'


@case('scope.agent_caller_by_ancestry', SCOPE_CAPS)
def _(ctx):
    ctx.seed_reads()
    agent_caller(ctx, CALLER)
    for label, args, kind in DIRECT_ONLY:
        res = ctx.ctl(*args)
        error(ctx, res, refusal_code(kind, 'agent_caller'), label=label)
        text = json.dumps(res.doc)
        ctx.check(A not in text and MISSING not in text, f'{label}: a refused caller learns nothing about which ids exist [contract]')
    unknown = ctx.ctl('task-events', MISSING, '--json')
    known = ctx.ctl('task-events', A, '--json')
    ctx.eq((unknown.doc or {}).get('error', {}).get('code'), (known.doc or {}).get('error', {}).get('code'),
           'authority precedes the task lookup: known and unknown ids refuse with the same code [contract]')
    ctx.eq((unknown.error or {}).get('message'), (known.error or {}).get('message'),
           'authority precedes the task lookup: known and unknown ids refuse identically [contract]')
    # Reads that managed workflow callers are scoped on stay open to a standalone task caller (pending U2).
    task = success(ctx, ctx.ctl('status', A, '--json'))['task']
    ctx.eq(task['task_id'], A, 'a standalone agent caller reads another task through status [baseline, pending U2]')
    exposed = ('last_output_tail', 'summary', 'raw_stream_log', 'events_log', 'resume_command', 'messages', 'recovery_activity')
    ctx.eq([key for key in exposed if key not in task], [], 'status exposes output tail, summary, log paths, resume command, messages and recovery activity to it [baseline, pending U2]')
    ctx.eq({row['task_id'] for row in success(ctx, ctx.ctl('list', '--json'))['tasks']} >= ALL_IDS, True,
           'list shows every task to a standalone agent caller [baseline, pending U2]')
    ctx.eq(success(ctx, ctx.ctl('recovery-activity', A, '--json')).get('task_id'), A, 'recovery-activity is open to it [baseline, pending U2]')
    ctx.eq([row['task_id'] for row in success(ctx, ctx.ctl('status-batch', '--task-ids', A, '--json'))['tasks']], [A],
           'status-batch is open to it [baseline, pending U2]')
    own = success(ctx, ctx.ctl('task-artifacts', CALLER, '--json'))
    ctx.eq((own['task_id'], own['state'], own['count']), (CALLER, 'available', 0), 'a task caller reads its own artifacts [contract]')
    byprefix = success(ctx, ctx.ctl('task-artifacts', CALLER[:8], '--json'))
    ctx.eq((byprefix['task_id'], byprefix.get('resolved_from')), (CALLER, {'input': CALLER[:8], 'task_id': CALLER}),
           'a prefix resolves inside the caller\'s own singleton set [contract]')
    error(ctx, ctx.ctl('task-artifacts', A, '--json'), 'artifact_error', label='a task caller may not read another task\'s artifacts')
    page = page_ready(ctx, 'task-list-page', '--json')
    ctx.eq(set(ids(page.result['items'])), ALL_IDS | {CALLER}, 'a standalone task caller sees the unscoped page [baseline, pending U2]')


@case('scope.environment_markers', SCOPE_CAPS)
def _(ctx):
    ctx.seed_reads()
    for env in ({'HARFLOW_TASK_ID': MISSING}, {'HARFLOW_TASK_ID': A}, {'PB_TASK_ID': MISSING}, {'HARFLOW_DEPTH': '1'},
                {'HARFLOW_DEPTH': 'x'}, {'PB_DEPTH': '2'}, {'HARFLOW_TASK_ID': ''}, {'HARFLOW_DEPTH': ''}, {'PB_TASK_ID': ''}, {'PB_DEPTH': ''}):
        label = ','.join(f'{k}={v[:8]}' for k, v in env.items())
        for name, args, kind in DIRECT_ONLY:
            error(ctx, ctx.ctl(*args, env=env), refusal_code(kind, 'agent_caller'), label=f'{name} [{label}]')
    # Presence is the test for the direct-only gate, truthiness for the managed reader.
    for env in ({'HARFLOW_TASK_ID': ''}, {'HARFLOW_DEPTH': ''}, {'PB_TASK_ID': ''}):
        for args in (['status', A, '--json'], ['list', '--json']):
            success(ctx, ctx.ctl(*args, env=env), f'{args[0]} with an empty marker [{",".join(env)}] is read normally [baseline]')


@case('scope.unverifiable_task_identity', SCOPE_CAPS)
def _(ctx):
    ctx.seed_reads()
    env = {'HARFLOW_TASK_ID': MISSING}
    for args in (['status', A, '--json'], ['list', '--json'], ['status-batch', '--task-ids', A, '--json'], ['recovery-activity', A, '--json']):
        error(ctx, ctx.ctl(*args, env=env), 'workflow_read_refused', label=args[0] + ' with an unverifiable HARFLOW_TASK_ID')
    for args in (['task-list-page', '--json'], ['task-list-page', '--json', '--task-ids', A]):
        error(ctx, settled_page(ctx, *args, env=env), 'invalid_params', label='task-list-page with an unverifiable HARFLOW_TASK_ID')
    error(ctx, ctx.ctl('task-artifacts', A, '--json', env=env), 'artifact_error', label='task-artifacts with an unverifiable HARFLOW_TASK_ID')
    for args in (['status', A, '--json'], ['list', '--json']):
        success(ctx, ctx.ctl(*args, env={'HARFLOW_TASK_ID': None}), 'unset marker reads normally')
    for env in ({'HARFLOW_DEPTH': '1'}, {'PB_TASK_ID': MISSING}):
        label = ','.join(env)
        for args in (['status', A, '--json'], ['list', '--json']):
            success(ctx, ctx.ctl(*args, env=env), f'{args[0]} with only {label} set')
    agent_caller(ctx, '34343434-3434-4434-8434-343434343434')
    ctx.eq(ctx.ctl('status', A, '--json', env={'HARFLOW_TASK_ID': MISSING}).rc, 0,
           'a marker naming an unknown task is overridden by a confirmed ancestry caller')


IDENTITY_AGENT = (('alive', 'running'), ('alive_with_markers', 'running'), ('alive', 'failed_unobserved'))
IDENTITY_DIRECT = (('reused', 'running'), ('legacy_markers_unseen', 'running'), ('alive', 'completed_observed'))
IDENTITY_UNDECIDABLE = (('markers_missing', 'running'), ('legacy_markers_seen', 'running'), ('legacy_no_markers', 'running'))


@case('scope.identity_verdicts', SCOPE_CAPS)
def _(ctx):
    """Fail-closed identity branches. Each variant registers this process under a different identity state."""
    for expectation, variants in (('agent', IDENTITY_AGENT), ('direct', IDENTITY_DIRECT), ('undecidable', IDENTITY_UNDECIDABLE)):
        for identity, status in variants:
            tag = f'{identity}/{status}'
            ctx.fx.remove_root()
            ctx.seed_reads()
            if not agent_caller(ctx, CALLER, identity=identity, status=status):
                ctx.skip(f'the fixture adapter does not express the identity state {tag}')
                continue
            events = ctx.ctl('task-events', A, '--json')
            interaction = ctx.ctl('interaction-events', '--json')
            status_res = ctx.ctl('status', A, '--json')
            listing = ctx.ctl('list', '--json')
            page = settled_page(ctx, 'task-list-page', '--json')
            artifacts = ctx.ctl('task-artifacts', A, '--json')
            label = f'[{tag}]'
            if expectation == 'agent':
                error(ctx, events, 'agent_caller', label=f'task-events {label}')
                error(ctx, interaction, 'unauthorized', label=f'interaction-events {label}')
                success(ctx, status_res, f'status {label}')
                success(ctx, listing, f'list {label}')
                success(ctx, page, f'task-list-page {label}')
                error(ctx, artifacts, 'artifact_error', label=f'task-artifacts of another task {label}')
            elif expectation == 'direct':
                for res, name in ((events, 'task-events'), (interaction, 'interaction-events'), (status_res, 'status'),
                                  (listing, 'list'), (page, 'task-list-page'), (artifacts, 'task-artifacts')):
                    success(ctx, res, f'{name} {label} (a stale or terminal record is not a caller)')
            else:
                error(ctx, events, 'caller_undecidable', label=f'task-events {label}')
                error(ctx, interaction, 'unauthorized', label=f'interaction-events {label}')
                error(ctx, status_res, 'workflow_read_refused', label=f'status {label}')
                error(ctx, listing, 'workflow_read_refused', label=f'list {label}')
                error(ctx, page, 'invalid_params', label=f'task-list-page {label}')
                error(ctx, artifacts, 'artifact_error', label=f'task-artifacts {label}')


@case('scope.incompatible_metadata_fails_closed', SCOPE_CAPS)
def _(ctx):
    """U4: one damaged task document fails every authority-gated read closed (baseline, owner decision pending)."""
    ctx.seed_reads()
    ctx.fx.damage_task(BROKEN)
    ctx.rebase()
    for args in (['list', '--json'], ['status', A, '--json'], ['status-batch', '--task-ids', A, '--json'], ['recovery-activity', A, '--json']):
        error(ctx, ctx.ctl(*args), 'workflow_read_refused', label=args[0] + ' with unverifiable history [baseline, U4]')
    # The contract says status reports task_incompatible for a damaged document; the caller check refuses first.
    error(ctx, ctx.ctl('status', BROKEN, '--json'), 'workflow_read_refused', label='status of the damaged task (task_incompatible is unreachable) [baseline, U4]')
    for name, args, kind in DIRECT_ONLY:
        code = 'caller_undecidable' if kind == 'caller_refusal' else 'unauthorized'
        error(ctx, ctx.ctl(*args), code, label=name + ' with unverifiable history [baseline, U4]')
    error(ctx, ctx.ctl('task-artifacts', A, '--json'), 'artifact_error', label='task-artifacts with unverifiable history [baseline, U4]')
    res = settled_page(ctx, 'task-list-page', '--json')
    page = success(ctx, res)
    ctx.eq((page.get('items'), page.get('authority_incomplete'), page.get('history_incomplete'), page.get('has_more'), page.get('next_cursor')),
           ([], True, True, False, None), 'the page read withholds all items when the catalog is incomplete [contract]')
    ctx.check(isinstance(page.get('note'), str) and page['note'], 'the withheld page carries a note [contract]')
    ctx.check(set(page.get('catalog_state', {})) <= {'status', 'source', 'reason'}, 'withheld page exposes only the allowlisted catalog state [contract]')


# --- cursor binding -----------------------------------------------------------------------------------

@case('cursors.binding', CORE + EV + ST + IE)
def _(ctx):
    ctx.seed_reads()
    other = ctx.root.parent / 'state-other'
    ctx.fx.at(other).seed_world(WORLD_READS)
    elsewhere = {'HARFLOW_STATE_ROOT': str(other)}
    cursor = page_ready(ctx, 'task-list-page', '--json', '--limit', '2').result['next_cursor']
    page_ready(ctx, 'task-list-page', '--json', env=elsewhere)
    for label, extra, env in (('another state root', [], elsewhere), ('a different active filter', ['--active-only'], None),
                              ('a different session filter', ['--session-id', 'x'], None),
                              ('a different predecessor scope', ['--predecessor-task-id', A], None)):
        error(ctx, ctx.ctl('task-list-page', '--json', '--limit', '2', '--cursor', cursor, *extra, env=env), 'invalid_params',
              label=f'task-list-page cursor under {label}')
    ctx.eq(ids(success(ctx, ctx.ctl('task-list-page', '--json', '--limit', '3', '--cursor', cursor))['items']), [C, B, A],
           'a page cursor survives a change of limit')
    token = stats_ready(ctx, 'statistics-page', *WINDOW, '--limit', '2', '--json').result['next_cursor']
    error(ctx, ctx.ctl('statistics-page', *WINDOW, '--limit', '2', '--json', '--cursor', token, env=elsewhere), 'invalid_cursor',
          label='statistics cursor under another state root [contract]')
    error(ctx, ctx.ctl('statistics-page', '--since', '2026-01-02T00:00:00+00:00', '--before', '2026-02-01T00:00:00+00:00',
                       '--limit', '2', '--json', '--cursor', token), 'invalid_cursor', label='statistics cursor under other bounds [contract]')
    ctx.fx.add_task({'task_id': '12345678-0000-4000-8000-0000000000aa', 'started_at': '2026-01-15T10:00:00+00:00',
                     'finished_at': '2026-01-15T10:01:00+00:00', 'status': 'completed', 'exit_code': 0, 'prompt': 'p'})
    ctx.rebase()
    error(ctx, ctx.ctl('statistics-page', *WINDOW, '--limit', '2', '--json', '--cursor', token), 'snapshot_expired',
          label='statistics cursor after a relevant mutation [contract]')
    event_cursor = success(ctx, ctx.ctl('task-events', A, '--json', '--from', 'oldest', '--limit', '2'))['newer_cursor']
    error(ctx, ctx.ctl('task-events', A, '--json', '--cursor', event_cursor, env=elsewhere), 'event_cursor_expired',
          label='event cursor under another state root [contract]')
    feed = success(ctx, ctx.ctl('interaction-events', '--json', '--limit', '2'))['next_cursor']
    moved = ctx.ctl('interaction-events', '--json', '--limit', '2', '--cursor', feed, env=elsewhere)
    ctx.eq(moved.rc, 0, 'an interaction-events cursor is not bound to the state root [baseline, U6]')


# --- reads against a missing root ---------------------------------------------------------------------

@case('reads.missing_root', SCOPE_CAPS)
def _(ctx):
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
        ctx.fx.remove_root()
        label = ' '.join(args[:1]) + (' successors' if '--predecessor-task-id' in args else '') + ' on a missing root'
        res = settled_page(ctx, *args) if args[0] == 'task-list-page' and '--predecessor-task-id' not in args else ctx.ctl(*args)
        if empty is not None:
            result = success(ctx, res, label)
            ctx.eq(result.get(key), empty, f'{label}: answers as empty [baseline, pending U10]')
        elif key == 'status-batch':
            doc = success(ctx, res, label)
            ctx.eq((doc['tasks'], doc['errors'][A]['code']), ([], 'unknown_task'), f'{label}: every id is unknown [baseline, pending U10]')
        else:
            error(ctx, res, key, label=label)
        ctx.eq(ctx.fx.root_state(), 'initialized', f'{label}: an ordinary read initializes the selected root [contract, pending U10]')
    ctx.fx.remove_root()
    ctx.baseline = None
