"""Bounded retained-history observations through public CLI contracts only."""


def statistics(c):
    from datetime import datetime, timedelta, timezone
    from .oracles import require, statistics_pages, cost_fact
    from .scenarios import definition

    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=1)).isoformat()
    before = (now + timedelta(days=1)).isoformat()
    base = ['statistics-page', '--since', since, '--before', before, '--limit', '1']
    readiness = []

    def first_page():
        def observe():
            document = c.call(base)
            state = document.get('result', {}).get('state')
            require(state in ('preparing', 'ready'), 'statistics unavailable in bounded fresh profile')
            readiness.append(state)
            return document
        return c.poll(observe, lambda d: d['result'].get('state') == 'ready')

    def pages(first=None):
        documents = [first if first is not None else first_page()]
        for _ in range(99):
            page = documents[-1]['result']
            if page.get('has_more') is False:
                return documents, statistics_pages(documents)
            cursor = page.get('next_cursor')
            require(isinstance(cursor, str) and bool(cursor), 'statistics next cursor missing')
            documents.append(c.call([*base, '--cursor', cursor]))
        require(False, 'statistics bounded 100-page traversal exhausted')

    # Establish owned compatible state through a public definition write, while
    # retaining genuinely empty execution history. No store marker is authored.
    c.save(definition('s09-stats-workflow'))
    empty_docs, empty = pages()
    require(empty == [], 'fresh statistics profile was not empty')
    empty_coverage = empty_docs[0]['result']
    for key in ('accounting_coverage', 'cost_coverage'):
        require(empty_coverage[key] == {'eligible_executions': 0, 'available_executions': 0},
                'qualified empty statistics coverage differs')

    # Run the workflow before standalone fakes: workflow_evidence associates every
    # OS-observed fake in this fresh profile, so unrelated earlier fakes cannot fit.
    root = c.start('s09-stats-workflow')
    workflow = c.workflow_evidence(root)
    known = {task['task_id'] for task in workflow['tasks']}
    require(len(known) == len(workflow['tasks']) and bool(known), 'workflow task identities ambiguous')
    proofs = []

    def complete(**spec):
        task = c.launch(**spec)
        status = c.terminal(task)
        raw = c.raw(task, completed=True)
        frames, ledger = c.frames(task['fake'])
        proofs.append({'task_id': task['task_id'], 'fake_pid': task['fake']['pid'],
                       'status': status, 'raw': raw, 'ledger': ledger})
        known.add(task['task_id'])
        return task, frames

    silent, _ = complete()
    reported, frames = complete(reported_cost=.125, reported_model='s09-reported-model')
    require(any(direction == 'stdout' and frame.get('subtype') == 'init' and
                frame.get('model') == 's09-reported-model' for direction, frame in frames) and
            any(direction == 'stdout' and frame.get('type') == 'result' and
                frame.get('total_cost_usd') == .125 for direction, frame in frames),
            'synthetic cost/model not corroborated by raw output')
    actor, _ = complete(mode='actor', operation='statistics-page', since=since, before=before)
    managed = c.managed_evidence(actor['fake'], [*base, '--json'], code='agent_caller')

    gate = c.p.workspace / 'statistics-active-release'
    active = c.launch('hold', gate=str(gate), survive_eof=True, hold_seconds=120)
    try:
        c.poll(lambda: c.status(active), lambda s: s.get('status') == 'running')
        c.poll(lambda: c.frames(active['fake'])[0], lambda rows: any(
            direction == 'stdout' and frame.get('type') == 'assistant' and
            any(part.get('text') == 'fixture ready' for part in frame.get('message', {}).get('content', []))
            for direction, frame in rows))

        def qualify(documents, facts):
            executions = [fact for fact in facts if fact['kind'] == 'execution']
            roots = [fact for fact in facts if fact['kind'] == 'workflow']
            require(len(roots) == 1 and roots[0].get('workflow_run_id') == root and
                    roots[0].get('root_workflow_run_id') == root,
                    'statistics finished root cohort missing or duplicated')
            require({fact.get('task_id') for fact in executions} == known and
                    len(executions) == len(known) and active['task_id'] not in known,
                    'statistics execution membership differs from externally known terminal tasks')
            indexed = {fact['task_id']: fact for fact in executions}
            for task in workflow['tasks']:
                fact = indexed[task['task_id']]
                require(fact.get('root_workflow_run_id') == root and
                        fact.get('workflow_run_id') == task['run_id'] and
                        fact.get('cohort_started_at') == roots[0].get('cohort_started_at'),
                        'workflow control/node execution did not join root cohort')
            for fact in executions:
                require(fact.get('status') == 'completed', 'statistics terminal status differs')
                if fact['task_id'] == reported['task_id']:
                    cost_fact(fact, reported_cost=.125, reported_model='s09-reported-model')
                else:
                    cost_fact(fact)
            coverage = documents[0]['result']['cost_coverage']
            require(coverage == {'eligible_executions': len(known), 'available_executions': 1},
                    'whole-snapshot synthetic cost coverage differs')
            require(sum(fact['cost_usd'] for fact in executions if fact['cost_additive']) == .125,
                    'additive synthetic cost sum differs')
            return indexed

        documents, facts = pages()
        qualify(documents, facts)
        stale_cursor = documents[0]['result'].get('next_cursor')
        require(isinstance(stale_cursor, str) and bool(stale_cursor), 'stale cursor case needs real pagination')
        additional, _ = complete()
        expired = c.call([*base, '--cursor', stale_cursor], refused=True, code='snapshot_expired')
        refreshed, refreshed_facts = pages()
        qualify(refreshed, refreshed_facts)
        result = {'bounds': {'since': since, 'before': before}, 'readiness_states': readiness,
                  'empty_coverage': {key: empty_coverage[key] for key in
                                     ('accounting_coverage', 'cost_coverage')},
                  'pages': documents, 'facts': facts, 'refreshed_pages': refreshed,
                  'refreshed_facts': refreshed_facts, 'snapshot_expired': expired,
                  'terminal_task_ids': sorted(known), 'excluded_active_task_id': active['task_id'],
                  'workflow': workflow, 'managed': managed, 'standalone_proofs': proofs,
                  'coverage': {key: refreshed[0]['result'][key] for key in
                               ('accounting_coverage', 'cost_coverage', 'retention')},
                  'limits': ['UTC bounds fixed once; retained fresh profile only',
                             'readiness <=40 calls/25 seconds; pagination <=100 pages',
                             'synthetic cost/model; no native deduplication or cross-date proof',
                             'no large history, retention, or MCP dispatcher restart qualification']}
    finally:
        # This changes the known cohort only after the last authoritative read.
        gate.write_text('release controlled statistics fixture\n')
    result['post_snapshot_active_settlement'] = {
        'task_id': active['task_id'], 'status': c.terminal(active),
        'raw': c.raw(active, completed=True), 'included_in_snapshot': False}
    return result
