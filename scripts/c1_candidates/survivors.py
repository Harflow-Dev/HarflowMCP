"""Candidate task-survivors-v1: named survivors, slot retention and human release (#118, plan D12)."""
import copy

from owner_runtime_profile.validation import Rejected, integer, process, require, supported, token
from .common import canonical, enums, specimen

NAME = 'task-survivors-v1'
STATES = 'none_observed present unknown'
INVENTORY = 'complete partial unknown'
ITEM_STATES = 'alive dead unknown'
MAX_ITEMS = 32


def identity(value):
    return process(value) and token(value.get('generation'), 32)


def survivor_state(items, inventory):
    """present when any survivor is alive; unknown when evidence is incomplete or unsettled; else none_observed."""
    if any(item['state'] == 'alive' for item in items):
        return 'present'
    if inventory != 'complete' or any(item['state'] == 'unknown' for item in items):
        return 'unknown'
    return 'none_observed'


def report(kind, value):
    document = value['document']
    block = document.get('survivors')
    require(isinstance(block, dict) and isinstance(block.get('items'), list) and type(block.get('truncated')) is bool, 'fields')
    enums(block, {'state': STATES, 'inventory': INVENTORY})
    require(len(block['items']) <= MAX_ITEMS, 'budget')
    for item in block['items']:
        require(isinstance(item, dict) and isinstance(item.get('name'), str) and 0 < len(item['name']) <= 64
                and identity(item.get('identity')) and isinstance(item.get('state'), str), 'fields')
        supported(item['state'] in ITEM_STATES.split(), 'enum')
    require(not block['truncated'] or block['inventory'] == 'partial', 'inconsistent_state')
    if block['state'] != survivor_state(block['items'], block['inventory']):
        raise Rejected('invalid', 'inconsistent_state')
    return {'parse': 'valid', 'state': block['state']}


def retention(kind, value):
    data = value.get('input')
    require(isinstance(data, dict), 'fields')
    enums(data, {'survivors': STATES, 'inventory': INVENTORY, 'slot': 'held cleanup'}, ('human_release', 'confirm_unsettled'))
    clear = data['survivors'] == 'none_observed' and data['inventory'] == 'complete'
    if data['human_release'] and not data['confirm_unsettled']:
        result = {'slot': 'retained', 'reason': 'confirmation_required', 'settlement': 'not_implied', 'replay': 'not_authorized'}
    elif data['human_release']:
        result = {'slot': 'released', 'reason': 'human_release', 'settlement': 'not_implied', 'replay': 'not_authorized'}
    elif clear:
        result = {'slot': 'release_eligible', 'reason': 'no_survivors', 'settlement': 'not_applicable', 'replay': 'not_applicable'}
    elif data['survivors'] == 'present':
        result = {'slot': 'retained', 'reason': 'survivors_present', 'settlement': 'not_applicable', 'replay': 'not_applicable'}
    else:
        result = {'slot': 'retained', 'reason': 'survivors_unknown', 'settlement': 'not_applicable', 'replay': 'not_applicable'}
    document = value['document']
    require(isinstance(document.get('slot'), dict), 'fields')
    enums(document['slot'], {'state': 'retained release_eligible released', 'reason': 'confirmation_required human_release no_survivors survivors_present survivors_unknown',
                             'settlement': 'not_implied not_applicable', 'replay': 'not_authorized not_applicable'})
    claimed = {'slot': document['slot']['state'], 'reason': document['slot']['reason'],
               'settlement': document['slot']['settlement'], 'replay': document['slot']['replay']}
    if claimed != result:
        raise Rejected('invalid', 'projection_mismatch')
    return {'parse': 'valid', **result}


def signal(kind, value):
    data = value.get('input')
    require(isinstance(data, dict) and identity(data.get('recorded')), 'fields')
    observed = data.get('observed')
    require('observed' in data and (observed is None or identity(observed)), 'fields')
    keys = ('pid', 'start_seconds', 'start_microseconds', 'boot_uuid')
    if observed is None:
        decision, reason = 'refuse', 'identity_unknown'
    elif any(observed[key] != data['recorded'][key] for key in keys):
        decision, reason = 'refuse', 'identity_mismatch'
    elif observed['generation'] != data['recorded']['generation']:
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


def block(state, inventory, items, truncated=False):
    return {'survivors': {'state': state, 'inventory': inventory, 'items': items, 'truncated': truncated}}


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
    add('report-unknown-item-is-unknown', 'report', block('unknown', 'complete', [gone, uncertain]), {'parse': 'valid', 'state': 'unknown'})
    add('report-alive-beats-partial', 'report', block('present', 'partial', [gradle], True), {'parse': 'valid', 'state': 'present'})
    reject('report-empty-partial-claims-none', specimen(NAME, 'report', block('none_observed', 'partial', [])), 'invalid', 'inconsistent_state')
    reject('report-alive-claims-none', specimen(NAME, 'report', block('none_observed', 'complete', [gradle])), 'invalid', 'inconsistent_state')
    reject('report-truncated-complete', specimen(NAME, 'report', block('present', 'complete', [gradle], True)), 'invalid', 'inconsistent_state')
    reject('report-over-budget', specimen(NAME, 'report', block('present', 'partial', [dict(gradle, identity=proc(5000 + n)) for n in range(33)], True)), 'invalid', 'budget')
    bare = copy.deepcopy(gradle); del bare['identity']['generation']
    reject('report-identity-without-generation', specimen(NAME, 'report', block('present', 'complete', [bare])), 'invalid', 'fields')
    future = copy.deepcopy(gradle); future['state'] = 'zombie'
    reject('report-future-item-state', specimen(NAME, 'report', block('present', 'complete', [future])), 'unsupported', 'enum')

    def slot(state, reason, settlement, replay):
        return {'slot': {'state': state, 'reason': reason, 'settlement': settlement, 'replay': replay}}

    def retained(name, survivors, inventory, state, reason, settlement, replay, human=False, confirm=False, slot_state='held'):
        data = {'survivors': survivors, 'inventory': inventory, 'slot': slot_state, 'human_release': human, 'confirm_unsettled': confirm}
        add(name, 'retention', slot(state, reason, settlement, replay), {'parse': 'valid', 'slot': state, 'reason': reason, 'settlement': settlement, 'replay': replay}, data)

    retained('retention-clear-is-eligible', 'none_observed', 'complete', 'release_eligible', 'no_survivors', 'not_applicable', 'not_applicable')
    retained('retention-survivors-hold-slot', 'present', 'complete', 'retained', 'survivors_present', 'not_applicable', 'not_applicable')
    retained('retention-unknown-holds-slot', 'unknown', 'complete', 'retained', 'survivors_unknown', 'not_applicable', 'not_applicable')
    retained('retention-partial-holds-slot', 'none_observed', 'partial', 'retained', 'survivors_unknown', 'not_applicable', 'not_applicable', slot_state='cleanup')
    retained('retention-release-needs-confirmation', 'present', 'complete', 'retained', 'confirmation_required', 'not_implied', 'not_authorized', human=True)
    retained('retention-human-release', 'present', 'complete', 'released', 'human_release', 'not_implied', 'not_authorized', human=True, confirm=True)
    retained('retention-human-release-unknown', 'unknown', 'partial', 'released', 'human_release', 'not_implied', 'not_authorized', human=True, confirm=True)
    early = specimen(NAME, 'retention', slot('release_eligible', 'no_survivors', 'not_applicable', 'not_applicable'),
                     {'survivors': 'present', 'inventory': 'complete', 'slot': 'held', 'human_release': False, 'confirm_unsettled': False})
    reject('retention-early-release-is-refused', early, 'invalid', 'projection_mismatch')
    implied = specimen(NAME, 'retention', slot('released', 'human_release', 'settled', 'not_authorized'),
                       {'survivors': 'present', 'inventory': 'complete', 'slot': 'held', 'human_release': True, 'confirm_unsettled': True})
    reject('retention-release-cannot-claim-settlement', implied, 'unsupported', 'enum')
    replay = specimen(NAME, 'retention', slot('released', 'human_release', 'not_implied', 'not_applicable'),
                      {'survivors': 'present', 'inventory': 'complete', 'slot': 'held', 'human_release': True, 'confirm_unsettled': True})
    reject('retention-release-cannot-authorize-replay', replay, 'invalid', 'projection_mismatch')
    flag = specimen(NAME, 'retention', slot('retained', 'survivors_present', 'not_applicable', 'not_applicable'),
                    {'survivors': 'present', 'inventory': 'complete', 'slot': 'held', 'human_release': 1, 'confirm_unsettled': False})
    reject('retention-boolean-as-number', flag, 'invalid', 'fields')
    current = proc(4001)
    for name, observed, decision, reason in (
            ('signal-current-identity', current, 'permit', 'identity_current'),
            ('signal-pid-reused', {**current, 'start_seconds': current['start_seconds'] + 9}, 'refuse', 'identity_mismatch'),
            ('signal-other-boot', {**current, 'boot_uuid': '87654321-1234-1234-1234-123456789abc'}, 'refuse', 'identity_mismatch'),
            ('signal-stale-generation', {**current, 'generation': '8' * 32}, 'refuse', 'generation_stale'),
            ('signal-unobservable', None, 'refuse', 'identity_unknown')):
        add(name, 'signal', {'decision': decision, 'reason': reason}, {'parse': 'valid', 'decision': decision, 'reason': reason},
            {'recorded': current, 'observed': observed})
    reject('signal-permit-after-reuse', specimen(NAME, 'signal', {'decision': 'permit', 'reason': 'identity_current'},
                                                 {'recorded': current, 'observed': {**current, 'start_seconds': 1}}), 'invalid', 'projection_mismatch')
    reject('signal-missing-observed', specimen(NAME, 'signal', {'decision': 'refuse', 'reason': 'identity_unknown'}, {'recorded': current}), 'invalid', 'fields')
    reject('envelope-future-version', {**specimen(NAME, 'report', block('none_observed', 'complete', [])), 'v': 2}, 'unsupported', 'envelope')
    return out


CANDIDATE = {'name': NAME, 'directory': 'contracts/candidates/task-survivors-v1', 'capability': 'task-survivors.v1',
             'kinds': ('report', 'retention', 'signal'), 'evaluate': evaluate, 'cases': cases,
             'specs': ('docs/decisions/C1-task-survivors.md',),
             'decision_basis': {'plan_D12': 'recommended_not_approved', 'bug_ledger': 'MCP #118', 'release_route_D15': 'undecided'}}
