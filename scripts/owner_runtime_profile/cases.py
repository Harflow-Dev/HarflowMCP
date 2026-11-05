"""Hand-authored specimens and independent expected decision oracles."""
import copy
import json


def canonical(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=True, indent=2, allow_nan=False) + '\n').encode()


def base(kind, data):
    return {'format': 'harflow.owner-runtime.profile', 'v': 1, 'kind': kind,
            'binding': {'operation': '1' * 32, 'session': '2' * 32, 'reservation': '3' * 32,
                        'attempt': '4' * 32, 'generation': '5' * 32,
                        'owner': {'pid': 42, 'start_seconds': 1, 'start_microseconds': 0,
                                  'boot_uuid': '12345678-1234-1234-1234-123456789abc'},
                        'root': {'device': 0, 'inode': 1, 'protocol': 5},
                        'request_digest': 'a' * 64, 'configuration_digest': 'b' * 64, 'steering': None},
            'data': data}


def fixtures():
    cases = {}
    def add(name, value, expected):
        cases[name + '.json'] = (value if isinstance(value, bytes) else canonical(value), expected)
    def invalid(name, value, reason='fields', parse='invalid'):
        add(name, value, {'parse': parse, 'reason': reason})
    def decision(name, value, choice, reason):
        add(name, value, {'parse': 'valid', 'decision': choice, 'reason': reason})
    def changed(value, **updates):
        result = copy.deepcopy(value)
        result['data'].update(updates)
        return result
    policy = base('policy', {'profile_version': 1})
    add('policy-default', policy, {'parse': 'valid', 'decision': 'accept', 'background_requirement': 'required', 'permission_mode': 'os_enforced', 'background_idle_seconds': 600, 'tool_level_opt_in': False})
    add('policy-explicit-boundary', changed(policy, background_requirement='incidental', permission_mode='tool_level', background_idle_seconds=21600), {'parse': 'valid', 'decision': 'accept', 'background_requirement': 'incidental', 'permission_mode': 'tool_level', 'background_idle_seconds': 21600, 'tool_level_opt_in': True})
    add('policy-unknown-flag', changed(policy, tool_level_opt_in=True), {'parse': 'valid', 'decision': 'accept', 'background_requirement': 'required', 'permission_mode': 'os_enforced', 'background_idle_seconds': 600, 'tool_level_opt_in': False})
    for name, updates in [('zero', {'background_idle_seconds': 0}), ('bool', {'background_idle_seconds': True}), ('over', {'background_idle_seconds': 21601})]:
        invalid('policy-' + name, changed(policy, **updates))
    invalid('policy-future', changed(policy, profile_version=2), 'enum', 'unsupported')
    invalid('policy-future-and-bad', changed(policy, profile_version=2, background_idle_seconds=False))
    missing = copy.deepcopy(policy); del missing['data']['profile_version']
    invalid('policy-missing', missing)
    permission = base('permission', {'requested_mode': 'os_enforced', 'established_mode': 'os_enforced', 'tool_level_opt_in': False, 'qualification': 'passed', **{key: 'proven' for key in ('binary_match', 'configuration_match', 'grants_match', 'protected_paths_match')}})
    decision('permission-qualified', permission, 'admit', 'qualified')
    decision('permission-tool-qualified', changed(permission, requested_mode='tool_level', established_mode='tool_level', tool_level_opt_in=True), 'admit', 'qualified')
    for name, updates, reason in [('opt-in', {'requested_mode': 'tool_level'}, 'opt_in_required'), ('mismatch', {'binary_match': 'mismatch'}, 'qualification_mismatch'), ('unknown', {'grants_match': 'unknown'}, 'unqualified'), ('failed', {'qualification': 'failed'}, 'unqualified'), ('mode', {'established_mode': 'unknown'}, 'mode_mismatch')]:
        decision('permission-' + name, changed(permission, **updates), 'refuse', reason)
    invalid('permission-future', changed(permission, qualification='future'), 'enum', 'unsupported')
    invalid('permission-structure-before-enum', changed(permission, qualification='future', tool_level_opt_in=1))
    obs = base('observation', {'agent_result': 'succeeded', 'result_binding': 'matched', 'sticky_failure': False, 'turn_open': False, 'requirement': 'required', 'obligations': 'satisfied', 'inventory': 'complete', 'exit': 'zero', 'stdout': 'eof', 'stderr': 'eof', 'input': 'reconciled', 'capture': 'lossless', 'watermark': 'complete', 'containment': 'complete', 'writer': 'settled', 'claims': 'reconciled', 'execution': 'healthy'})
    def observation(name, updates, result, workflow, release, reason):
        add('observation-' + name, changed(obs, **updates), {'parse': 'valid', 'result': result, 'workflow': workflow, 'release': release, 'reason': reason})
    observation('settled', {}, 'succeeded', 'succeeded', 'evidence_complete', 'succeeded')
    for name, updates in [('partial', {'inventory': 'partial'}), ('unknown', {'obligations': 'unknown'}), ('unknown-required', {'requirement': 'unknown', 'obligations': 'pending'})]:
        observation(name, updates, 'succeeded', 'blocked', 'blocked', 'obligations')
    observation('incidental-pending', {'requirement': 'incidental', 'obligations': 'pending'}, 'succeeded', 'succeeded', 'blocked', 'succeeded')
    observation('incidental-failed', {'requirement': 'incidental', 'obligations': 'failed'}, 'succeeded', 'succeeded', 'evidence_complete', 'succeeded')
    observation('required-failed', {'obligations': 'failed'}, 'succeeded', 'failed', 'evidence_complete', 'required_obligation_failed')
    observation('sticky-failure', {'sticky_failure': True, 'result_binding': 'unknown'}, 'failed', 'failed', 'evidence_complete', 'result_failed')
    observation('open-failed', {'agent_result': 'failed', 'turn_open': True}, 'failed', 'blocked', 'blocked', 'unsettled')
    observation('open-turn', {'turn_open': True}, 'unknown', 'blocked', 'blocked', 'unsettled')
    observation('stale-result', {'result_binding': 'mismatched'}, 'unknown', 'blocked', 'blocked', 'result_unknown')
    observation('capture-gap', {'capture': 'known_loss'}, 'succeeded', 'blocked', 'blocked', 'unsettled')
    observation('execution-unknown', {'execution': 'unknown'}, 'succeeded', 'blocked', 'blocked', 'execution_unknown')
    observation('execution-failed', {'exit': 'nonzero'}, 'succeeded', 'failed', 'evidence_complete', 'execution_failed')
    observation('cancelled', {'execution': 'cancelled'}, 'succeeded', 'cancelled', 'evidence_complete', 'cancelled')
    observation('containment-unknown', {'containment': 'unknown'}, 'succeeded', 'succeeded', 'blocked', 'succeeded')
    pending = {'sequence': 1, 'operation': '1' * 32, 'generation': '5' * 32, 'attempt': '4' * 32, 'channel_nonce': '6' * 32, 'writer': copy.deepcopy(obs['binding']['owner']), 'binding_digest': 'c' * 64, 'command_digest': 'd' * 64, 'command': 'append', 'expected_revision': 7, 'payload_bytes': 16384}
    ack = {key: copy.deepcopy(value) for key, value in pending.items() if key not in ('command', 'expected_revision', 'payload_bytes')}
    ack.update(revision=8, state='committed')
    storage = base('storage', {'pending': pending, 'ack': ack, 'send_state': 'complete', 'deadline': 'current', 'poisoned': False})
    acknowledged = copy.deepcopy(storage)
    acknowledged['data']['ack']['writer']['optional_note'] = 'ignored metadata'
    decision('storage-acknowledged', acknowledged, 'advance', 'acknowledged')
    for name, updates, reason in [('poisoned', {'poisoned': True}, 'poisoned'), ('partial', {'send_state': 'partial'}, 'uncertain_send'), ('late', {'deadline': 'expired'}, 'late_ack')]:
        decision('storage-' + name, changed(storage, **updates), 'retain', reason)
    for name, updates, reason in [('refused-old-revision', {'state': 'refused', 'revision': 1}, 'storage_refusal'), ('unknown-null-revision', {'state': 'unknown', 'revision': None}, 'commit_unknown'), ('stale-attempt', {'attempt': '9' * 32, 'state': 'refused'}, 'identity_mismatch'), ('revision', {'revision': 7}, 'revision_mismatch')]:
        specimen = copy.deepcopy(storage); specimen['data']['ack'].update(updates)
        decision('storage-' + name, specimen, 'retain', reason)
    specimen = copy.deepcopy(storage); specimen['data']['ack']['revision'] = None
    invalid('storage-committed-null', specimen)
    specimen = copy.deepcopy(storage); specimen['data']['pending']['payload_bytes'] = 16385
    invalid('storage-oversize-payload', specimen)
    specimen = copy.deepcopy(storage); specimen['data']['pending']['expected_revision'] = (1 << 64) - 1
    decision('storage-overflow', specimen, 'retain', 'revision_mismatch')
    for proof, action in [('agent_result', 'withdraw'), ('storage_ack', 'replace'), ('observer_attachment', 'release')]:
        decision('steering-' + proof, base('steering_guard', {'claimed_proof': proof, 'requested_action': action}), 'refuse', 'not_steering_authority')
    specimen = copy.deepcopy(policy); specimen['binding']['root']['protocol'] = 6
    invalid('binding-future', specimen, 'binding', 'unsupported')
    del specimen['binding']['owner']
    invalid('binding-missing-before-future', specimen, 'binding')
    specimen = copy.deepcopy(policy); specimen['v'] = 2
    invalid('envelope-future', specimen, 'envelope', 'unsupported')
    specimen['data'] = []
    invalid('envelope-structure-before-future', specimen, 'envelope')
    invalid('encoding-duplicate', b'{"v":1,"v":1}', 'encoding')
    invalid('encoding-nonfinite', b'{"x":NaN}', 'encoding')
    invalid('encoding-surrogate', b'{"x":"\\ud800"}', 'encoding')
    invalid('encoding-utf8', b'\xff', 'encoding')
    invalid('budget-bytes', b'x' * 24576 + b'\n', 'budget')
    invalid('budget-array', changed(policy, extra=[0] * 65), 'budget')
    invalid('budget-string', changed(policy, extra='x' * 4097), 'budget')
    nested = 0
    for _ in range(14):
        nested = [nested]
    boundary = changed(policy, extra=nested)
    add('budget-depth-boundary', boundary, {'parse': 'valid', 'decision': 'accept', 'background_requirement': 'required', 'permission_mode': 'os_enforced', 'background_idle_seconds': 600, 'tool_level_opt_in': False})
    invalid('budget-depth-exceeded', changed(policy, extra=[nested]), 'budget')
    specimen = copy.deepcopy(policy); specimen['kind'] = 'future'
    invalid('envelope-kind-future', specimen, 'envelope', 'unsupported')
    specimen = copy.deepcopy(policy); specimen['binding']['owner']['pid'] = True
    invalid('binding-boolean-pid', specimen, 'binding')
    specimen = copy.deepcopy(storage)
    specimen['data']['pending'].update(command='close', payload_bytes=0)
    specimen['data']['ack']['revision'] = 7
    decision('storage-close-same-revision', specimen, 'advance', 'acknowledged')
    return cases
