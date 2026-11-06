"""Candidate task-result-v1: agent result separated from background obligations and settlement (#110)."""
import copy

from owner_runtime_profile.models import OBSERVATION, observation
from owner_runtime_profile.validation import Rejected, require
from .common import canonical, enums, specimen

NAME = 'task-result-v1'
BOOLEANS = ('sticky_failure', 'turn_open')
RESULT_ENUMS = {'agent_result': 'unknown succeeded failed'}
BACKGROUND = {'requirement': 'required incidental unknown', 'obligations': 'pending satisfied failed unknown',
              'inventory': 'complete partial unknown'}
SETTLEMENT = {'workflow': 'blocked succeeded failed cancelled', 'release': 'blocked evidence_complete',
              'reason': 'unsettled obligations execution_unknown cancelled execution_failed result_failed '
                        'required_obligation_failed succeeded result_unknown'}


def project(data):
    """The public fields derived from the owner-runtime observation inputs (the model of that candidate)."""
    outcome = observation(data)
    return {'agent_result': outcome['result'],
            'background': {key: data[key] for key in BACKGROUND},
            'settlement': {'workflow': outcome['workflow'], 'reason': outcome['reason'], 'release': outcome['release']}}


def evaluate(kind, value):
    data = value.get('input')
    require(isinstance(data, dict), 'fields')
    enums(data, OBSERVATION, BOOLEANS)
    document = value['document']
    require(isinstance(document.get('background'), dict) and isinstance(document.get('settlement'), dict), 'fields')
    enums(document, RESULT_ENUMS)
    enums(document['background'], BACKGROUND)
    enums(document['settlement'], SETTLEMENT)
    expected = project(data)
    actual = {'agent_result': document['agent_result'],
              'background': {key: document['background'][key] for key in BACKGROUND},
              'settlement': {key: document['settlement'][key] for key in ('workflow', 'reason', 'release')}}
    if actual != expected:
        raise Rejected('invalid', 'projection_mismatch')
    return {'parse': 'valid', 'agent_result': expected['agent_result'], 'workflow': expected['settlement']['workflow'],
            'release': expected['settlement']['release'], 'reason': expected['settlement']['reason']}


SETTLED = {'agent_result': 'succeeded', 'result_binding': 'matched', 'sticky_failure': False, 'turn_open': False,
           'requirement': 'required', 'obligations': 'satisfied', 'inventory': 'complete', 'exit': 'zero',
           'stdout': 'eof', 'stderr': 'eof', 'input': 'reconciled', 'capture': 'lossless', 'watermark': 'complete',
           'containment': 'complete', 'writer': 'settled', 'claims': 'reconciled', 'execution': 'healthy'}


def document(result, requirement, obligations, inventory, workflow, reason, release):
    return {'agent_result': result,
            'background': {'requirement': requirement, 'obligations': obligations, 'inventory': inventory},
            'settlement': {'workflow': workflow, 'reason': reason, 'release': release}}


def cases():
    out = {}

    def add(name, updates, doc, expected_workflow=None):
        data = {**SETTLED, **updates}
        raw = specimen(NAME, 'projection', doc, data)
        valid = {'parse': 'valid', 'agent_result': doc['agent_result'], 'workflow': doc['settlement']['workflow'],
                 'release': doc['settlement']['release'], 'reason': doc['settlement']['reason']}
        out[name + '.json'] = (raw, valid)

    def bad(name, raw, parse, reason):
        out[name + '.json'] = (raw if isinstance(raw, bytes) else canonical(raw), {'parse': parse, 'reason': reason})

    add('settled', {}, document('succeeded', 'required', 'satisfied', 'complete', 'succeeded', 'succeeded', 'evidence_complete'))
    # The #110 case: a leftover background command failed at the idle bound; the agent result stays succeeded.
    add('incidental-failed-keeps-success', {'requirement': 'incidental', 'obligations': 'failed'},
        document('succeeded', 'incidental', 'failed', 'complete', 'succeeded', 'succeeded', 'evidence_complete'))
    add('incidental-pending-holds-release', {'requirement': 'incidental', 'obligations': 'pending'},
        document('succeeded', 'incidental', 'pending', 'complete', 'succeeded', 'succeeded', 'blocked'))
    add('required-failed', {'obligations': 'failed'},
        document('succeeded', 'required', 'failed', 'complete', 'failed', 'required_obligation_failed', 'evidence_complete'))
    add('required-pending-blocks', {'obligations': 'pending'},
        document('succeeded', 'required', 'pending', 'complete', 'blocked', 'obligations', 'blocked'))
    add('unknown-requirement-never-incidental', {'requirement': 'unknown', 'obligations': 'pending'},
        document('succeeded', 'unknown', 'pending', 'complete', 'blocked', 'obligations', 'blocked'))
    add('partial-inventory-is-not-empty', {'inventory': 'partial'},
        document('succeeded', 'required', 'satisfied', 'partial', 'blocked', 'obligations', 'blocked'))
    add('sticky-failure-wins', {'sticky_failure': True, 'result_binding': 'unknown'},
        document('failed', 'required', 'satisfied', 'complete', 'failed', 'result_failed', 'evidence_complete'))
    add('stale-result-is-unknown', {'result_binding': 'mismatched'},
        document('unknown', 'required', 'satisfied', 'complete', 'blocked', 'result_unknown', 'blocked'))
    add('execution-failure-not-hidden-by-incidental', {'requirement': 'incidental', 'obligations': 'satisfied', 'exit': 'nonzero'},
        document('succeeded', 'incidental', 'satisfied', 'complete', 'failed', 'execution_failed', 'evidence_complete'))
    add('cancelled', {'execution': 'cancelled'},
        document('succeeded', 'required', 'satisfied', 'complete', 'cancelled', 'cancelled', 'evidence_complete'))
    add('open-pipe-is-unsettled', {'stdout': 'open'},
        document('succeeded', 'required', 'satisfied', 'complete', 'blocked', 'unsettled', 'blocked'))
    # The #110 shapes this candidate does NOT unblock: a background command that keeps the output pipe open
    # holds settlement (an open stream is `unsettled` whatever the requirement), and a caller that never
    # declared the requirement leaves it `unknown`, which behaves as required, so a failed leftover still fails the task.
    add('held-pipe-blocks-settlement', {'requirement': 'incidental', 'obligations': 'pending', 'stdout': 'open'},
        document('succeeded', 'incidental', 'pending', 'complete', 'blocked', 'unsettled', 'blocked'))
    add('undeclared-requirement-failed-obligation-fails', {'requirement': 'unknown', 'obligations': 'failed'},
        document('succeeded', 'unknown', 'failed', 'complete', 'failed', 'required_obligation_failed', 'evidence_complete'))
    add('open-turn-hides-success', {'turn_open': True},
        document('unknown', 'required', 'satisfied', 'complete', 'blocked', 'unsettled', 'blocked'))
    lie = specimen(NAME, 'projection', document('succeeded', 'incidental', 'failed', 'complete', 'failed', 'execution_failed', 'evidence_complete'),
                   {**SETTLED, 'requirement': 'incidental', 'obligations': 'failed'})
    bad('document-contradicts-input', lie, 'invalid', 'projection_mismatch')
    rewrite = specimen(NAME, 'projection', document('failed', 'incidental', 'failed', 'complete', 'failed', 'result_failed', 'evidence_complete'),
                       {**SETTLED, 'requirement': 'incidental', 'obligations': 'failed'})
    bad('incidental-failure-cannot-rewrite-result', rewrite, 'invalid', 'projection_mismatch')
    future = specimen(NAME, 'projection', document('succeeded', 'required', 'satisfied', 'complete', 'succeeded', 'succeeded', 'evidence_complete'), SETTLED)
    future['document']['agent_result'] = 'partial'
    bad('document-future-enum', future, 'unsupported', 'enum')
    missing = copy.deepcopy(future)
    missing['document']['agent_result'] = 'succeeded'
    del missing['document']['background']['inventory']
    bad('document-missing-inventory', missing, 'invalid', 'fields')
    boolean = copy.deepcopy(missing)
    boolean['document']['background']['inventory'] = 'complete'
    boolean['input']['sticky_failure'] = 0
    bad('input-boolean-as-number', boolean, 'invalid', 'fields')
    version = copy.deepcopy(boolean)
    version['input']['sticky_failure'] = False
    version['v'] = 2
    bad('envelope-future-version', version, 'unsupported', 'envelope')
    kind = copy.deepcopy(version)
    kind['v'] = 1
    kind['kind'] = 'stream'
    bad('envelope-future-kind', kind, 'unsupported', 'envelope')
    other = copy.deepcopy(kind)
    other['kind'] = 'projection'
    other['candidate'] = 'task-survivors-v1'
    bad('envelope-foreign-candidate', other, 'invalid', 'envelope')
    return out


CANDIDATE = {'name': NAME, 'directory': 'contracts/candidates/task-result-v1', 'capability': 'task-result-separation.v1',
             'kinds': ('projection',), 'evaluate': evaluate, 'cases': cases,
             'specs': ('docs/decisions/C1-task-result-separation.md',),
             'decision_basis': {'owner_runtime_decision_2': 'approved', 'bug_ledger': 'MCP #110'}}
