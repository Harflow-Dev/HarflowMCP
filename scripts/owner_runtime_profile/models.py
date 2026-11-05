"""Normative, hypothetical decision tables, independent from product runtime."""
from .validation import (U64, require, supported, integer, token, process,
                         enum_fields, check_enums)

PERMISSION = {
    'requested_mode': 'os_enforced tool_level', 'established_mode': 'os_enforced tool_level unknown',
    'qualification': 'passed failed unknown unmeasured',
    **{key: 'proven unknown mismatch' for key in ('binary_match', 'configuration_match', 'grants_match', 'protected_paths_match')},
}
OBSERVATION = {
    'agent_result': 'unknown succeeded failed', 'result_binding': 'matched mismatched unknown',
    'requirement': 'required incidental unknown', 'obligations': 'pending satisfied failed unknown',
    'inventory': 'complete partial unknown', 'exit': 'unobserved zero nonzero signal',
    'stdout': 'eof open unknown error', 'stderr': 'eof open unknown error',
    'input': 'reconciled pending unknown error', 'capture': 'lossless known_loss unknown error',
    'watermark': 'complete gap unknown', 'containment': 'complete incomplete unknown',
    'writer': 'settled live unknown', 'claims': 'reconciled pending unknown',
    'execution': 'healthy failed cancelled unknown',
}
COMMANDS = 'bootstrap claim record_child create_raw accept capture_begin append known_loss finish close'
INCREMENTS = {'claim', 'record_child', 'accept', 'append', 'known_loss', 'finish'}

def outcome(decision, reason):
    return {'parse': 'valid', 'decision': decision, 'reason': reason}

def policy(data):
    require(integer(data.get('profile_version')), 'fields')
    require(all(key not in data or isinstance(data[key], str) for key in ('background_requirement', 'permission_mode')), 'fields')
    idle = data.get('background_idle_seconds', 600)
    require(type(idle) in (int, float) and 0 < idle <= 21600, 'fields')
    supported(data['profile_version'] == 1, 'enum')
    supported(data.get('background_requirement', 'required') in ('required', 'incidental') and data.get('permission_mode', 'os_enforced') in ('os_enforced', 'tool_level'), 'enum')
    return {'parse': 'valid', 'decision': 'accept', 'background_requirement': data.get('background_requirement', 'required'), 'permission_mode': data.get('permission_mode', 'os_enforced'), 'background_idle_seconds': idle, 'tool_level_opt_in': data.get('permission_mode') == 'tool_level'}

def permission(data):
    enum_fields(data, PERMISSION, ('tool_level_opt_in',))
    check_enums(data, PERMISSION)
    matches = [data[key] for key in PERMISSION if key.endswith('_match')]
    if data['requested_mode'] == 'tool_level' and not data['tool_level_opt_in']:
        return outcome('refuse', 'opt_in_required')
    if 'mismatch' in matches:
        return outcome('refuse', 'qualification_mismatch')
    if data['qualification'] != 'passed' or 'unknown' in matches:
        return outcome('refuse', 'unqualified')
    if data['established_mode'] != data['requested_mode']:
        return outcome('refuse', 'mode_mismatch')
    return outcome('admit', 'qualified')

def observation(data):
    enum_fields(data, OBSERVATION, ('sticky_failure', 'turn_open'))
    check_enums(data, OBSERVATION)
    result = data['agent_result']
    if data['sticky_failure']:
        result = 'failed'
    elif data['result_binding'] != 'matched':
        result = 'unknown'
    elif result != 'failed' and data['turn_open']:
        result = 'unknown'
    unsettled = (data['exit'] == 'unobserved' or data['stdout'] != 'eof' or data['stderr'] != 'eof'
                 or data['input'] != 'reconciled' or data['capture'] != 'lossless'
                 or data['watermark'] != 'complete' or data['writer'] != 'settled' or data['turn_open'])
    required = data['requirement'] != 'incidental'
    if unsettled:
        workflow, reason = 'blocked', 'unsettled'
    elif data['inventory'] != 'complete' or data['obligations'] == 'unknown' or (required and data['obligations'] == 'pending'):
        workflow, reason = 'blocked', 'obligations'
    elif data['execution'] == 'unknown':
        workflow, reason = 'blocked', 'execution_unknown'
    elif data['execution'] == 'cancelled':
        workflow, reason = 'cancelled', 'cancelled'
    elif data['execution'] == 'failed' or data['exit'] in ('nonzero', 'signal'):
        workflow, reason = 'failed', 'execution_failed'
    elif result == 'failed':
        workflow, reason = 'failed', 'result_failed'
    elif required and data['obligations'] == 'failed':
        workflow, reason = 'failed', 'required_obligation_failed'
    elif result == 'succeeded':
        workflow, reason = 'succeeded', 'succeeded'
    else:
        workflow, reason = 'blocked', 'result_unknown'
    release = (workflow != 'blocked' and data['containment'] == 'complete' and data['claims'] == 'reconciled'
               and data['inventory'] == 'complete' and data['obligations'] in ('satisfied', 'failed'))
    return {'parse': 'valid', 'result': result, 'workflow': workflow, 'release': 'evidence_complete' if release else 'blocked', 'reason': reason}

def storage(data, binding):
    enum_fields(data, {'send_state': '', 'deadline': ''}, ('poisoned',))
    pending, ack = data.get('pending'), data.get('ack')
    for item in (pending, ack):
        require(isinstance(item, dict) and integer(item.get('sequence'), 1) and process(item.get('writer')), 'fields')
        require(all(token(item.get(key), 32) for key in ('operation', 'generation', 'attempt', 'channel_nonce')), 'fields')
        require(all(token(item.get(key), 64) for key in ('binding_digest', 'command_digest')), 'fields')
    require(isinstance(pending.get('command'), str) and integer(pending.get('expected_revision')) and integer(pending.get('payload_bytes'), 0, 16384), 'fields')
    require(isinstance(ack.get('state'), str) and 'revision' in ack and (ack['revision'] is None or integer(ack['revision'])), 'fields')
    require(ack['state'] != 'committed' or ack['revision'] is not None, 'fields')
    require(pending['command'] == 'append' or pending['payload_bytes'] == 0, 'fields')
    check_enums(data, {'send_state': 'complete partial unknown', 'deadline': 'current expired'})
    supported(pending['command'] in COMMANDS.split() and ack['state'] in ('committed', 'refused', 'unknown'), 'enum')
    if data['poisoned']:
        return outcome('retain', 'poisoned')
    if data['send_state'] != 'complete':
        return outcome('retain', 'uncertain_send')
    if data['deadline'] == 'expired':
        return outcome('retain', 'late_ack')
    identity = ('sequence', 'operation', 'generation', 'attempt', 'channel_nonce', 'binding_digest', 'command_digest')
    process_keys = ('pid', 'start_seconds', 'start_microseconds', 'boot_uuid')
    writer_mismatch = any(pending['writer'][key] != ack['writer'][key] for key in process_keys)
    if writer_mismatch or any(pending[key] != ack[key] for key in identity) or any(pending[key] != binding[key] for key in ('operation', 'generation', 'attempt')):
        return outcome('retain', 'identity_mismatch')
    if ack['state'] != 'committed':
        return outcome('retain', 'commit_unknown' if ack['state'] == 'unknown' else 'storage_refusal')
    revision = pending['expected_revision'] + (pending['command'] in INCREMENTS)
    if revision > U64 or ack['revision'] != revision:
        return outcome('retain', 'revision_mismatch')
    return outcome('advance', 'acknowledged')

def steering_guard(data):
    enums = {'claimed_proof': 'agent_result storage_ack observer_attachment', 'requested_action': 'withdraw retire replace release control'}
    enum_fields(data, enums)
    check_enums(data, enums)
    return outcome('refuse', 'not_steering_authority')
