"""Candidate task-survivors-v1: named survivors, slot retention and human release (#118, plan D12 - not approved)."""
import copy

from owner_runtime_profile.validation import Rejected, process, require, supported, token
from .common import canonical, enums, specimen

NAME = 'task-survivors-v1'
STATES = 'none_observed present unknown'
INVENTORY = 'complete partial unknown'
ITEM_STATES = 'alive dead unknown'
BASIS = ('process_group', 'session', 'descendant_tree', 'containment_complete')
COMPLETE_NEEDS = frozenset(('process_group', 'descendant_tree', 'containment_complete'))
MAX_ITEMS = 32
MAX_NAME_BYTES = 64
SETTLEMENT = {'containment': 'complete incomplete unknown', 'writer': 'settled live unknown', 'claims': 'reconciled pending unknown'}


def os_identity(value):
    return process(value)


def identity(value):
    return process(value) and token(value.get('generation'), 32)


def valid_name(value):
    return isinstance(value, str) and 0 < len(value.encode('utf-8')) <= MAX_NAME_BYTES


def survivor_state(items, inventory):
    """present when any survivor is alive; unknown when evidence is incomplete or unsettled; else none_observed."""
    if any(item['state'] == 'alive' for item in items):
        return 'present'
    if inventory != 'complete' or any(item['state'] == 'unknown' for item in items):
        return 'unknown'
    return 'none_observed'


def report(kind, value):
    block = value['document'].get('survivors')
    require(isinstance(block, dict) and isinstance(block.get('items'), list) and type(block.get('truncated')) is bool
            and isinstance(block.get('basis'), list), 'fields')
    enums(block, {'state': STATES, 'inventory': INVENTORY})
    require(len(block['items']) <= MAX_ITEMS, 'budget')
    require(all(isinstance(b, str) for b in block['basis']) and len(set(block['basis'])) == len(block['basis']), 'fields')
    supported(set(block['basis']) <= set(BASIS), 'enum')
    for item in block['items']:
        require(isinstance(item, dict) and valid_name(item.get('name')) and identity(item.get('identity')) and isinstance(item.get('state'), str), 'fields')
        supported(item['state'] in ITEM_STATES.split(), 'enum')
    require(not block['truncated'] or block['inventory'] == 'partial', 'inconsistent_state')
    # `complete` is only attainable with a basis that covers more than the process group: descendants and a
    # containment the owner-runtime model reports as complete. A process-group scan alone stays `partial`.
    require(block['inventory'] != 'complete' or COMPLETE_NEEDS <= set(block['basis']), 'inventory_basis')
    if block['state'] != survivor_state(block['items'], block['inventory']):
        raise Rejected('invalid', 'inconsistent_state')
    return {'parse': 'valid', 'state': block['state']}


def retention(kind, value):
    data = value.get('input')
    require(isinstance(data, dict), 'fields')
    enums(data, {'survivors': STATES, 'inventory': INVENTORY, 'slot': 'held cleanup'}, ('human_release', 'confirm_unsettled'))
    evidence = data.get('settlement_evidence')
    enums(evidence, SETTLEMENT)
    require(data['survivors'] != 'none_observed' or data['inventory'] == 'complete', 'inconsistent_state')
    settled = evidence['containment'] == 'complete' and evidence['writer'] == 'settled' and evidence['claims'] == 'reconciled'
    held = {'settlement': 'not_applicable', 'replay': 'not_applicable'}
    if data['human_release'] and not data['confirm_unsettled']:
        result = {'slot': 'retained', 'reason': 'confirmation_required', 'settlement': 'not_implied', 'replay': 'not_authorized'}
    elif data['human_release']:
        result = {'slot': 'released', 'reason': 'human_release', 'settlement': 'not_implied', 'replay': 'not_authorized'}
    elif data['survivors'] == 'present':
        result = {'slot': 'retained', 'reason': 'survivors_present', **held}
    elif data['survivors'] == 'unknown' or data['inventory'] != 'complete':
        result = {'slot': 'retained', 'reason': 'survivors_unknown', **held}
    elif not settled:
        result = {'slot': 'retained', 'reason': 'settlement_evidence_incomplete', **held}
    else:
        result = {'slot': 'release_eligible', 'reason': 'no_survivors', **held}
    document = value['document']
    require(isinstance(document.get('slot'), dict), 'fields')
    enums(document['slot'], {'state': 'retained release_eligible released',
                             'reason': 'confirmation_required human_release no_survivors survivors_present survivors_unknown settlement_evidence_incomplete',
                             'settlement': 'not_implied not_applicable', 'replay': 'not_authorized not_applicable'})
    claimed = {'slot': document['slot']['state'], 'reason': document['slot']['reason'],
               'settlement': document['slot']['settlement'], 'replay': document['slot']['replay']}
    if claimed != result:
        raise Rejected('invalid', 'projection_mismatch')
    return {'parse': 'valid', **result}


def signal(kind, value):
    """Permit a signal only when the OS identity observed now matches the recorded one and the record's generation is current."""
    data = value.get('input')
    require(isinstance(data, dict) and identity(data.get('recorded')) and token(data.get('current_generation'), 32), 'fields')
    observed = data.get('observed')
    require('observed' in data and (observed is None or os_identity(observed)), 'fields')
    keys = ('pid', 'start_seconds', 'start_microseconds', 'boot_uuid')
    if observed is None:
        decision, reason = 'refuse', 'identity_unknown'
    elif any(observed[key] != data['recorded'][key] for key in keys):
        decision, reason = 'refuse', 'identity_mismatch'
    elif data['recorded']['generation'] != data['current_generation']:
        decision, reason = 'refuse', 'generation_stale'
    else:
        decision, reason = 'permit', 'identity_current'
    document = value['document']
    require(document.get('decision') in ('permit', 'refuse') and isinstance(document.get('reason'), str), 'fields')
    if (document['decision'], document['reason']) != (decision, reason):
        raise Rejected('invalid', 'projection_mismatch')
    return {'parse': 'valid', 'decision': decision, 'reason': reason}


def evaluate(kind, value):
    return {'report': report, 'retention': retention, 'signal': signal}[kind](kind, value)


def proc(pid, generation='7' * 32):
    return {'pid': pid, 'start_seconds': 1700000000 + pid, 'start_microseconds': 5, 'boot_uuid': '12345678-1234-1234-1234-123456789abc', 'generation': generation}


def os_proc(pid):
    return {key: value for key, value in proc(pid).items() if key != 'generation'}


FULL = ['process_group', 'descendant_tree', 'containment_complete']
SETTLED = {'containment': 'complete', 'writer': 'settled', 'claims': 'reconciled'}


def block(state, inventory, items, truncated=False, basis=None):
    return {'survivors': {'state': state, 'inventory': inventory, 'items': items, 'truncated': truncated,
                          'basis': FULL if basis is None and inventory == 'complete' else (basis or ['process_group'])}}


def cases():
    out = {}

    def add(name, kind, doc, expected, data=None):
        out[name + '.json'] = (specimen(NAME, kind, doc, data), expected)

    def reject(name, raw, parse, reason):
        out[name + '.json'] = (raw if isinstance(raw, bytes) else canonical(raw), {'parse': parse, 'reason': reason})

    gradle = {'name': 'gradle-daemon', 'identity': proc(4001), 'state': 'alive'}
    xcode = {'name': 'xcodebuild', 'identity': proc(4002), 'state': 'alive'}
    gone = {'name': 'gradle-daemon', 'identity': proc(4003), 'state': 'dead'}
    uncertain = {'name': 'unclassified', 'identity': proc(4004), 'state': 'unknown'}
    add('report-none-observed', 'report', block('none_observed', 'complete', []), {'parse': 'valid', 'state': 'none_observed'})
    add('report-all-dead', 'report', block('none_observed', 'complete', [gone]), {'parse': 'valid', 'state': 'none_observed'})
    add('report-named-survivors', 'report', block('present', 'complete', [gradle, xcode]), {'parse': 'valid', 'state': 'present'})
    add('report-partial-inventory-is-unknown', 'report', block('unknown', 'partial', [gone], True), {'parse': 'valid', 'state': 'unknown'})
    add('report-process-group-only-stays-partial', 'report', block('unknown', 'partial', [], False, ['process_group']), {'parse': 'valid', 'state': 'unknown'})
    add('report-unknown-item-is-unknown', 'report', block('unknown', 'complete', [gone, uncertain]), {'parse': 'valid', 'state': 'unknown'})
    add('report-alive-beats-partial', 'report', block('present', 'partial', [gradle], True), {'parse': 'valid', 'state': 'present'})
    multibyte = {'name': 'é' * 32, 'identity': proc(4005), 'state': 'alive'}          # 32 characters, exactly 64 UTF-8 bytes
    add('report-name-at-the-byte-limit', 'report', block('present', 'complete', [multibyte]), {'parse': 'valid', 'state': 'present'})
    add('report-name-in-cjk-within-limit', 'report', block('present', 'complete', [dict(multibyte, name='構' * 21)]), {'parse': 'valid', 'state': 'present'})
    reject('report-name-over-the-byte-limit', specimen(NAME, 'report', block('present', 'complete', [dict(multibyte, name='é' * 33)])), 'invalid', 'fields')
    reject('report-cjk-name-over-the-byte-limit', specimen(NAME, 'report', block('present', 'complete', [dict(multibyte, name='構' * 22)])), 'invalid', 'fields')
    reject('report-process-group-only-claims-complete', specimen(NAME, 'report', block('none_observed', 'complete', [], False, ['process_group'])), 'invalid', 'inventory_basis')
    reject('report-complete-without-containment', specimen(NAME, 'report', block('none_observed', 'complete', [], False, ['process_group', 'descendant_tree'])), 'invalid', 'inventory_basis')
    reject('report-empty-partial-claims-none', specimen(NAME, 'report', block('none_observed', 'partial', [])), 'invalid', 'inconsistent_state')
    reject('report-alive-claims-none', specimen(NAME, 'report', block('none_observed', 'complete', [gradle])), 'invalid', 'inconsistent_state')
    reject('report-truncated-complete', specimen(NAME, 'report', block('present', 'complete', [gradle], True)), 'invalid', 'inconsistent_state')
    reject('report-over-budget', specimen(NAME, 'report', block('present', 'partial', [dict(gradle, identity=proc(5000 + n)) for n in range(33)], True)), 'invalid', 'budget')
    bare = copy.deepcopy(gradle); del bare['identity']['generation']
    reject('report-identity-without-generation', specimen(NAME, 'report', block('present', 'complete', [bare])), 'invalid', 'fields')
    future = copy.deepcopy(gradle); future['state'] = 'zombie'
    reject('report-future-item-state', specimen(NAME, 'report', block('present', 'complete', [future])), 'unsupported', 'enum')
    futurebasis = block('present', 'complete', [gradle]); futurebasis['survivors']['basis'] = FULL + ['kernel_audit']
    reject('report-future-basis', specimen(NAME, 'report', futurebasis), 'unsupported', 'enum')

    def slot(state, reason, settlement, replay):
        return {'slot': {'state': state, 'reason': reason, 'settlement': settlement, 'replay': replay}}

    def retained(name, survivors, inventory, state, reason, settlement, replay, human=False, confirm=False, slot_state='held', evidence=None):
        data = {'survivors': survivors, 'inventory': inventory, 'slot': slot_state, 'human_release': human,
                'confirm_unsettled': confirm, 'settlement_evidence': evidence or SETTLED}
        add(name, 'retention', slot(state, reason, settlement, replay), {'parse': 'valid', 'slot': state, 'reason': reason, 'settlement': settlement, 'replay': replay}, data)

    na = ('not_applicable', 'not_applicable')
    retained('retention-clear-is-eligible', 'none_observed', 'complete', 'release_eligible', 'no_survivors', *na)
    retained('retention-survivors-hold-slot', 'present', 'complete', 'retained', 'survivors_present', *na)
    retained('retention-unknown-holds-slot', 'unknown', 'complete', 'retained', 'survivors_unknown', *na)
    retained('retention-partial-holds-slot', 'unknown', 'partial', 'retained', 'survivors_unknown', *na, slot_state='cleanup')
    for key, value in (('containment', 'unknown'), ('containment', 'incomplete'), ('writer', 'live'), ('writer', 'unknown'), ('claims', 'pending'), ('claims', 'unknown')):
        retained(f'retention-clear-but-{key}-{value}-holds-slot', 'none_observed', 'complete', 'retained', 'settlement_evidence_incomplete', *na,
                 evidence={**SETTLED, key: value})
    retained('retention-release-needs-confirmation', 'present', 'complete', 'retained', 'confirmation_required', 'not_implied', 'not_authorized', human=True)
    retained('retention-human-release', 'present', 'complete', 'released', 'human_release', 'not_implied', 'not_authorized', human=True, confirm=True)
    retained('retention-human-release-unknown', 'unknown', 'partial', 'released', 'human_release', 'not_implied', 'not_authorized', human=True, confirm=True,
             evidence={'containment': 'unknown', 'writer': 'unknown', 'claims': 'unknown'})
    base = {'slot': 'held', 'human_release': False, 'confirm_unsettled': False, 'settlement_evidence': SETTLED}
    reject('retention-none-with-partial-inventory-is-inconsistent',
           specimen(NAME, 'retention', slot('retained', 'survivors_unknown', *na), {**base, 'survivors': 'none_observed', 'inventory': 'partial'}), 'invalid', 'inconsistent_state')
    reject('retention-release-without-settlement-evidence',
           specimen(NAME, 'retention', slot('release_eligible', 'no_survivors', *na), {**base, 'survivors': 'none_observed', 'inventory': 'complete',
                                                                                   'settlement_evidence': {**SETTLED, 'claims': 'pending'}}), 'invalid', 'projection_mismatch')
    reject('retention-missing-settlement-evidence',
           specimen(NAME, 'retention', slot('release_eligible', 'no_survivors', *na), {k: v for k, v in {**base, 'survivors': 'none_observed', 'inventory': 'complete'}.items() if k != 'settlement_evidence'}), 'invalid', 'fields')
    reject('retention-early-release-is-refused', specimen(NAME, 'retention', slot('release_eligible', 'no_survivors', *na), {**base, 'survivors': 'present', 'inventory': 'complete'}), 'invalid', 'projection_mismatch')
    reject('retention-release-cannot-claim-settlement', specimen(NAME, 'retention', slot('released', 'human_release', 'settled', 'not_authorized'),
                                                                 {**base, 'survivors': 'present', 'inventory': 'complete', 'human_release': True, 'confirm_unsettled': True}), 'unsupported', 'enum')
    reject('retention-release-cannot-authorize-replay', specimen(NAME, 'retention', slot('released', 'human_release', 'not_implied', 'not_applicable'),
                                                                 {**base, 'survivors': 'present', 'inventory': 'complete', 'human_release': True, 'confirm_unsettled': True}), 'invalid', 'projection_mismatch')
    reject('retention-boolean-as-number', specimen(NAME, 'retention', slot('retained', 'survivors_present', *na),
                                                   {**base, 'survivors': 'present', 'inventory': 'complete', 'human_release': 1}), 'invalid', 'fields')
    recorded = proc(4001)
    current = recorded['generation']
    for name, observed, generation, decision, reason in (
            ('signal-current-identity', os_proc(4001), current, 'permit', 'identity_current'),
            ('signal-pid-reused', {**os_proc(4001), 'start_seconds': recorded['start_seconds'] + 9}, current, 'refuse', 'identity_mismatch'),
            ('signal-other-boot', {**os_proc(4001), 'boot_uuid': '87654321-1234-1234-1234-123456789abc'}, current, 'refuse', 'identity_mismatch'),
            ('signal-record-generation-superseded', os_proc(4001), '8' * 32, 'refuse', 'generation_stale'),
            ('signal-unobservable', None, current, 'refuse', 'identity_unknown')):
        add(name, 'signal', {'decision': decision, 'reason': reason}, {'parse': 'valid', 'decision': decision, 'reason': reason},
            {'recorded': recorded, 'observed': observed, 'current_generation': generation})
    reject('signal-permit-after-reuse', specimen(NAME, 'signal', {'decision': 'permit', 'reason': 'identity_current'},
                                                 {'recorded': recorded, 'observed': {**os_proc(4001), 'start_seconds': 1}, 'current_generation': current}), 'invalid', 'projection_mismatch')
    reject('signal-missing-observed', specimen(NAME, 'signal', {'decision': 'refuse', 'reason': 'identity_unknown'}, {'recorded': recorded, 'current_generation': current}), 'invalid', 'fields')
    reject('signal-missing-current-generation', specimen(NAME, 'signal', {'decision': 'refuse', 'reason': 'identity_unknown'}, {'recorded': recorded, 'observed': None}), 'invalid', 'fields')
    reject('envelope-future-version', {**specimen(NAME, 'report', block('none_observed', 'complete', [])), 'v': 2}, 'unsupported', 'envelope')
    return out


CANDIDATE = {'name': NAME, 'directory': 'contracts/candidates/task-survivors-v1', 'capability': 'task-survivors.v1',
             'kinds': ('report', 'retention', 'signal'), 'evaluate': evaluate, 'cases': cases,
             'specs': ('docs/decisions/C1-task-survivors.md',),
             'decision_basis': {'plan_D12': 'recommended_not_approved', 'bug_ledger': 'MCP #118', 'release_route_D15': 'undecided',
                                'confirm_unsettled': 'proposal'}}
