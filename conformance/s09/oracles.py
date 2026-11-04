"""Public-contract evidence checks; no engine imports or fake implementation calls."""
import hashlib
import json
import math

from conformance.oracles import ConformanceError, strict_json, validate_envelope


def require(value, message):
    if not value:
        raise ConformanceError(message)


def resource_pending(document, task_id=None):
    validate_envelope(document, 'harflow.ctl')
    error = document.get('error', {})
    data = error.get('data', {})
    require(error.get('code') == 'task_pending_launch' and
            data.get('launch_state') == 'waiting_for_resources',
            'resource wait needs a typed pending-launch response')
    if task_id is not None and 'task_id' in data:
        require(data['task_id'] == task_id, 'pending launch task identity mismatch')
    return data


def resource_inventory(document):
    validate_envelope(document, 'harflow.ctl')
    rows = document.get('resources')
    require(isinstance(rows, list), 'resource inventory missing')
    names = []
    for row in rows:
        require(isinstance(row, dict) and isinstance(row.get('name'), str), 'resource name missing')
        names.append(row['name'])
        require(type(row.get('capacity')) is int and row['capacity'] > 0, 'resource capacity unknown')
        for kind in ('holders', 'queue'):
            require(isinstance(row.get(kind), list), 'resource occupancy unknown')
            for item in row[kind]:
                require(isinstance(item, dict) and isinstance(item.get('reservation_id'), str)
                        and isinstance(item.get('owner'), dict), 'resource reservation identity missing')
    require(names == sorted(set(names)), 'resource inventory names unordered or duplicated')
    return rows


def resource_task_order(document, name, holders, queue):
    rows = [row for row in resource_inventory(document) if row['name'] == name]
    require(len(rows) == 1, 'resource scope absent or ambiguous')
    row = rows[0]
    for kind, expected in (('holders', holders), ('queue', queue)):
        owners = [item['owner'].get('task_id') for item in row[kind]]
        require(owners == list(expected), 'resource ' + kind + ' differs from expected task order')
    require(row['capacity'] == 1, 'fixture exclusive resource capacity changed')
    return row


def resource_spawn_order(events, pids):
    actual = [event.get('pid') for event in events if event.get('event') == 'spawn'
              and event.get('role') == 'fake']
    require(actual == list(pids) and len(set(actual)) == len(actual),
            'resource FIFO lacks matching external fake spawn order')


def workflow_terminal(run, observed_fakes, expected='completed'):
    require(isinstance(run, dict) and run.get('status') == expected,
            'workflow public status contradicts expected outcome')
    require(run.get('settling') is False, 'workflow settlement unknown or incomplete')
    require(bool(observed_fakes) and all(row.get('role') == 'fake' and row.get('exited') is True
            for row in observed_fakes), 'workflow fake claims lack OS exit evidence')
    return run


def workflow_input_matches(frames, session, public_prompt, role, receipt=None, *, original_request=None):
    """Match an issued control turn, not a reused session or original prompt alone.

    The public task prompt can be original prose or a checkpoint display label
    for orchestrators. Their actual optimized control input has a separate
    published delivery receipt and the run retains its original request.
    Workers receive a focused assignment inside their runtime role guidance.
    """
    if not isinstance(session, str) or not session or not any(
            direction == 'stdout' and frame.get('subtype') == 'init' and frame.get('session_id') == session
            for direction, frame in frames):
        return False
    inputs = [frame.get('text') for direction, frame in frames
              if direction == 'stdin' and frame.get('type') == 'input']
    if len(inputs) != 1 or not isinstance(inputs[0], str):
        return False
    if role != 'orchestrator':
        return isinstance(public_prompt, str) and bool(public_prompt) and public_prompt in inputs[0]
    if not isinstance(receipt, dict):
        return False
    try:
        control = strict_json(inputs[0].encode())
    except ConformanceError:
        return False
    if not isinstance(control, dict):
        return False
    delivery, checkpoint = control.get('delivery'), control.get('checkpoint')
    if not isinstance(delivery, dict) or not isinstance(checkpoint, dict):
        return False
    acknowledgement = delivery.get('acknowledgement')
    if not isinstance(acknowledgement, dict):
        return False
    mode = delivery.get('mode')
    if mode not in ('bootstrap', 'delta'):
        return False
    keys = ('version', 'scope', 'session_owner', 'revision', 'digest', 'base_revision')
    if (checkpoint.get('decision_id') != receipt.get('checkpoint') or
            not isinstance(receipt.get('checkpoint'), str) or not receipt['checkpoint'] or
            any(key not in acknowledgement or key not in receipt or acknowledgement[key] != receipt[key] for key in keys) or
            type(receipt.get('version')) is not int or receipt['version'] != 2 or
            not isinstance(receipt.get('session_owner'), str) or not receipt['session_owner'] or
            not isinstance(receipt.get('digest'), str) or type(receipt.get('revision')) is not int or receipt['revision'] < 1 or
            not isinstance(receipt.get('scope'), str) or
            checkpoint.get('workflow_run_id') != receipt['scope'] or
            delivery.get('mode') != receipt.get('delivery_mode')):
        return False
    if mode == 'bootstrap':
        if receipt['base_revision'] is not None or 'bootstrap' not in control:
            return False
    elif ('bootstrap' in control or type(receipt['base_revision']) is not int or
          not 1 <= receipt['base_revision'] < receipt['revision']):
        return False
    payload = {key: value for key, value in control.items() if key != 'delivery'}
    digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True,
                                      separators=(',', ':')).encode()).hexdigest()
    if digest != receipt['digest']:
        return False
    if mode == 'bootstrap':
        original = public_prompt if original_request is None else original_request
        bootstrap = control['bootstrap']
        return isinstance(original, str) and isinstance(bootstrap, dict) and isinstance(bootstrap.get('context'), dict) and bootstrap['context'].get('original_request') == original
    return True


def correlate_managed(rows, ledger, argv, parent_fake_pid, stdout_bytes):
    """A managed fixture's own capture stays untrusted without this OS conjunction."""
    require(ledger.get('argv') == list(argv), 'managed captured argv mismatch')
    require(type(ledger.get('returncode')) is int and ledger['returncode'] != 0 and
            ledger.get('pipes_drained') is True and ledger.get('timed_out') is False and
            ledger.get('capture_exceeded') is False and ledger.get('cleanup') in
            ('group_signalled', 'group_absent', 'group_quiescent_os_observed'),
            'managed refusal has unknown bounded outcome')
    require(any(row.get('role') == 'cli' and row.get('pid') == ledger.get('pid') and
            row.get('ppid') == parent_fake_pid and row.get('exited') is True for row in rows),
            'managed refusal lacks exact OS PID/parent/exit')
    stream = ledger.get('streams', {}).get('stdout', {})
    require(stream.get('captured_bytes') == stream.get('bytes_seen') == len(stdout_bytes) and
            stream.get('sha256') == hashlib.sha256(stdout_bytes).hexdigest(),
            'managed stdout byte/hash mismatch')
    document = strict_json(stdout_bytes)
    validate_envelope(document, 'harflow.ctl')
    require(isinstance(document.get('error'), dict), 'managed operation was not refused')
    return document


def statistics_pages(documents):
    """Return facts only for a complete, coverage-qualified single generation."""
    require(bool(documents), 'statistics page set missing')
    facts, seen, baseline = [], set(), None
    cursors = set()
    for index, document in enumerate(documents):
        validate_envelope(document, 'harflow.ctl')
        page = document.get('result', {})
        require(page.get('state') == 'ready' and page.get('history_complete') is True and
                page.get('ownership_complete') is True and page.get('retained_history_only') is True,
                'statistics history/ownership is not qualified')
        require(isinstance(page.get('generation'), str) and bool(page['generation']) and
                isinstance(page.get('snapshot_id'), str) and bool(page['snapshot_id']),
                'statistics snapshot identity missing')
        require(type(page.get('projection_version')) is int and page['projection_version'] > 0,
                'statistics projection identity missing')
        identity = {key: page.get(key) for key in ('generation', 'snapshot_id',
            'projection_version', 'accounting_coverage', 'cost_coverage', 'retention')}
        if baseline is None:
            baseline = identity
        require(identity == baseline, 'statistics generation or whole-snapshot coverage changed')
        for key in ('accounting_coverage', 'cost_coverage'):
            coverage = page.get(key, {})
            require(type(coverage.get('eligible_executions')) is int and
                    type(coverage.get('available_executions')) is int and
                    0 <= coverage['available_executions'] <= coverage['eligible_executions'],
                    'statistics coverage missing or invalid')
        more = index < len(documents) - 1
        require(page.get('has_more') is more, 'statistics page set is incomplete')
        cursor = page.get('next_cursor')
        if more:
            require(isinstance(cursor, str) and bool(cursor) and cursor not in cursors,
                    'statistics continuation cursor missing or repeated')
            cursors.add(cursor)
        else:
            require(cursor is None, 'statistics final page retains cursor')
        require(isinstance(page.get('items'), list), 'statistics facts missing')
        for fact in page['items']:
            require(isinstance(fact, dict) and isinstance(fact.get('id'), str) and
                    fact.get('kind') in ('workflow', 'execution'), 'statistics fact identity unknown')
            require(fact['id'] not in seen, 'statistics duplicate fact')
            seen.add(fact['id'])
            facts.append(fact)
    return facts


def cost_fact(fact, *, reported_cost=None, reported_model=None, requested_model=None):
    require(fact.get('kind') == 'execution' and 'requested_model' in fact and
            fact['requested_model'] == requested_model, 'requested model replaced or missing')
    required = ('cost_usd', 'cost_provenance', 'cost_additive', 'observed_model', 'observed_model_provenance')
    require(all(key in fact for key in required), 'capability-qualified cost/model fact missing')
    if reported_cost is None:
        require(fact['cost_usd'] is None and fact['cost_provenance'] is None and
                fact['cost_additive'] is False, 'unknown cost was fabricated or additive')
    else:
        value = fact['cost_usd']
        require(type(value) in (int, float) and math.isfinite(value) and value >= 0 and
                value == reported_cost and fact['cost_provenance'] == 'backend_reported' and
                fact['cost_additive'] is True, 'reported synthetic cost/provenance mismatch')
    require(fact['observed_model'] == reported_model and
            fact['observed_model_provenance'] ==
            ('harness_initialization_event' if reported_model is not None else None),
            'observed model/provenance mismatch')
    return fact
