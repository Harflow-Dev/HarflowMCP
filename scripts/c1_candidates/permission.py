"""Candidate permission-mode-report-v1: truthful permission-mode reporting and evidence admission (plan D6)."""
import copy

from owner_runtime_profile.validation import Rejected, require, supported, token
from .common import canonical, enums, specimen

NAME = 'permission-mode-report-v1'
MODES = 'os_enforced tool_level'
EVIDENCE_BINDING = ('binary_sha256', 'version', 'platform', 'probe_revision', 'grants_digest')


def evidence(value, with_state=True):
    require(isinstance(value, dict), 'fields')
    require(token(value.get('binary_sha256'), 64) and token(value.get('grants_digest'), 64), 'fields')
    require(all(isinstance(value.get(key), str) and 0 < len(value[key]) <= 128 for key in ('version', 'platform', 'probe_revision')), 'fields')
    if with_state:
        enums(value, {'state': 'passed failed unknown unmeasured'})


def report(kind, value):
    block = value['document'].get('enforcement')
    require(isinstance(block, dict) and type(block.get('os_enforced')) is bool and isinstance(block.get('freedom'), str), 'fields')
    permission = block.get('permission')
    enums(permission, {'requested_mode': MODES, 'established_mode': 'os_enforced tool_level unknown',
                       'opt_in_source': 'none explicit_request freedom_level_d6', 'ambient_configuration': 'uncontrolled'},
          ('tool_level_opt_in', 'inherited_rules_may_change_behaviour'))
    evidence(permission.get('evidence'))
    # Never overclaim: only an established, qualified OS boundary may report os_enforced.
    qualified = permission['evidence']['state'] == 'passed'
    require(block['os_enforced'] == (permission['established_mode'] == 'os_enforced' and qualified), 'overclaim')
    require(permission['requested_mode'] != 'tool_level' or permission['tool_level_opt_in'], 'opt_in')
    require(permission['tool_level_opt_in'] == (permission['opt_in_source'] != 'none'), 'opt_in')
    require(permission['inherited_rules_may_change_behaviour'] is True, 'disclosure')
    return {'parse': 'valid', 'os_enforced': block['os_enforced'], 'established_mode': permission['established_mode']}


def admission(kind, value):
    data = value.get('input')
    require(isinstance(data, dict), 'fields')
    enums(data, {'requested_mode': MODES}, ('tool_level_opt_in',))
    evidence(data.get('evidence'))
    evidence(data.get('current'), with_state=False)
    stale = any(data['evidence'][key] != data['current'][key] for key in EVIDENCE_BINDING)
    passed = data['evidence']['state'] == 'passed'
    if data['requested_mode'] == 'tool_level' and not data['tool_level_opt_in']:
        decision, reason = 'refuse', 'opt_in_required'
    elif data['requested_mode'] == 'os_enforced' and not passed:
        decision, reason = 'refuse', 'strict_unmeasured'
    elif stale:
        decision, reason = 'refuse', 'evidence_stale'
    elif not passed:
        decision, reason = 'refuse', 'unqualified'
    else:
        decision, reason = 'admit', 'qualified'
    document = value['document']
    require(document.get('decision') in ('admit', 'refuse') and isinstance(document.get('reason'), str), 'fields')
    if (document['decision'], document['reason']) != (decision, reason):
        raise Rejected('invalid', 'projection_mismatch')
    return {'parse': 'valid', 'decision': decision, 'reason': reason}


def evaluate(kind, value):
    return {'report': report, 'admission': admission}[kind](kind, value)


DIGEST_A, DIGEST_B = 'a' * 64, 'b' * 64
CURRENT = {'binary_sha256': DIGEST_A, 'version': '2.1.318', 'platform': 'darwin-arm64', 'probe_revision': 'r1', 'grants_digest': DIGEST_B}


def block(freedom='write_in_repo', os_enforced=False, requested='tool_level', established='tool_level', opt_in=True,
          source='freedom_level_d6', state='passed', **extra):
    permission = {'requested_mode': requested, 'established_mode': established, 'tool_level_opt_in': opt_in, 'opt_in_source': source,
                  'evidence': {**CURRENT, 'state': state}, 'ambient_configuration': 'uncontrolled',
                  'inherited_rules_may_change_behaviour': True}
    permission.update(extra)
    return {'enforcement': {'freedom': freedom, 'os_enforced': os_enforced, 'permission': permission}}


def cases():
    out = {}

    def add(name, kind, doc, expected, data=None):
        out[name + '.json'] = (specimen(NAME, kind, doc, data), expected)

    def reject(name, raw, parse, reason):
        out[name + '.json'] = (raw if isinstance(raw, bytes) else canonical(raw), {'parse': parse, 'reason': reason})

    def valid(osx, mode):
        return {'parse': 'valid', 'os_enforced': osx, 'established_mode': mode}

    add('report-stock-claude-tool-level', 'report', block(), valid(False, 'tool_level'))
    add('report-explicit-tool-level', 'report', block(source='explicit_request'), valid(False, 'tool_level'))
    add('report-unknown-establishment', 'report', block(established='unknown', state='unknown'), valid(False, 'unknown'))
    add('report-os-enforced-qualified', 'report', block(os_enforced=True, requested='os_enforced', established='os_enforced', opt_in=False, source='none'), valid(True, 'os_enforced'))
    reject('report-tool-level-claims-os', specimen(NAME, 'report', block(os_enforced=True)), 'invalid', 'overclaim')
    reject('report-unqualified-claims-os', specimen(NAME, 'report', block(os_enforced=True, requested='os_enforced', established='os_enforced', opt_in=False, source='none', state='unmeasured')), 'invalid', 'overclaim')
    reject('report-tool-level-without-opt-in', specimen(NAME, 'report', block(opt_in=False, source='none')), 'invalid', 'opt_in')
    reject('report-opt-in-without-source', specimen(NAME, 'report', block(source='none')), 'invalid', 'opt_in')
    reject('report-missing-disclosure', specimen(NAME, 'report', block(inherited_rules_may_change_behaviour=False)), 'invalid', 'disclosure')
    reject('report-ambient-claimed-controlled', specimen(NAME, 'report', block(ambient_configuration='controlled')), 'unsupported', 'enum')
    short = copy.deepcopy(block()); short['enforcement']['permission']['evidence']['binary_sha256'] = 'abc'
    reject('report-malformed-digest', specimen(NAME, 'report', short), 'invalid', 'fields')
    boolean = copy.deepcopy(block()); boolean['enforcement']['os_enforced'] = 0
    reject('report-boolean-as-number', specimen(NAME, 'report', boolean), 'invalid', 'fields')

    def admit(name, requested, opt_in, state, decision, reason, current=None):
        data = {'requested_mode': requested, 'tool_level_opt_in': opt_in, 'evidence': {**CURRENT, 'state': state}, 'current': current or CURRENT}
        add(name, 'admission', {'decision': decision, 'reason': reason}, {'parse': 'valid', 'decision': decision, 'reason': reason}, data)

    admit('admission-tool-level-qualified', 'tool_level', True, 'passed', 'admit', 'qualified')
    admit('admission-tool-level-needs-opt-in', 'tool_level', False, 'passed', 'refuse', 'opt_in_required')
    admit('admission-strict-unmeasured', 'os_enforced', False, 'unmeasured', 'refuse', 'strict_unmeasured')
    admit('admission-strict-failed', 'os_enforced', False, 'failed', 'refuse', 'strict_unmeasured')
    admit('admission-strict-qualified', 'os_enforced', False, 'passed', 'admit', 'qualified')
    for key, changed in (('binary_sha256', 'c' * 64), ('version', '2.1.319'), ('platform', 'linux-x86_64'), ('probe_revision', 'r2'), ('grants_digest', 'd' * 64)):
        admit('admission-stale-' + key.replace('_', '-'), 'tool_level', True, 'passed', 'refuse', 'evidence_stale', {**CURRENT, key: changed})
    admit('admission-unqualified', 'tool_level', True, 'unmeasured', 'refuse', 'unqualified')
    admit('admission-stale-before-unqualified', 'tool_level', True, 'failed', 'refuse', 'evidence_stale', {**CURRENT, 'probe_revision': 'r2'})
    admit('admission-ambient-change-is-not-binding', 'tool_level', True, 'passed', 'admit', 'qualified', {**CURRENT})
    wrong = specimen(NAME, 'admission', {'decision': 'admit', 'reason': 'qualified'},
                     {'requested_mode': 'tool_level', 'tool_level_opt_in': True, 'evidence': {**CURRENT, 'state': 'passed'}, 'current': {**CURRENT, 'version': '9'}})
    reject('admission-admits-stale-evidence', wrong, 'invalid', 'projection_mismatch')
    fallback = specimen(NAME, 'admission', {'decision': 'admit', 'reason': 'qualified'},
                        {'requested_mode': 'os_enforced', 'tool_level_opt_in': True, 'evidence': {**CURRENT, 'state': 'unmeasured'}, 'current': CURRENT})
    reject('admission-no-fallback-to-tool-level', fallback, 'invalid', 'projection_mismatch')
    reject('admission-future-mode', specimen(NAME, 'admission', {'decision': 'refuse', 'reason': 'x'},
                                              {'requested_mode': 'sandboxed', 'tool_level_opt_in': True, 'evidence': {**CURRENT, 'state': 'passed'}, 'current': CURRENT}), 'unsupported', 'enum')
    reject('envelope-future-kind', {**specimen(NAME, 'report', block()), 'kind': 'future'}, 'unsupported', 'envelope')
    return out


CANDIDATE = {'name': NAME, 'directory': 'contracts/candidates/permission-mode-report-v1', 'capability': 'permission-mode-report.v1',
             'kinds': ('report', 'admission'), 'evaluate': evaluate, 'cases': cases,
             'specs': ('docs/decisions/C1-permission-mode-report.md',),
             'decision_basis': {'plan_D6': 'approved', 'owner_runtime_decision_3': 'approved', 'bug_ledger': 'MCP #120 (disclosure only)'}}
