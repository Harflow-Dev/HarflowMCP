"""Bounded internal synthetic poststart documents; parsing grants no authority."""
from __future__ import annotations
import copy
import hashlib
import json
import math
import re
import uuid

MAX_DOCUMENT = 131072
PROOFS = set('qualified_profile original_enrollment complete_closure future_issuance_fence exit_reaped stdout_eof stderr_eof input_reconciled lossless_capture final_watermark writers_quiescent extents_reconciled handles_settled no_pending_mutation claims_settled resources_reconciled sessions_reconciled messages_reconciled release_journal_complete job_quiescent stale_callback_fence all_obligations_settled'.split())
REFS = set('root activation task attempt visit node candidate owner_generation containment_generation process_generation'.split())
REVISIONS = set('goal_revision interaction_revision control_revision target_revision'.split())
DIGESTS = set('definition_digest goal_digest assignment_digest configuration_digest grant_digest profile_digest verifier_digest closure_digest'.split())
COMMON = dict(format='harflow.steering.poststart.candidate', v=1, status='specified_unimplemented', provenance='synthetic_specification', python_runtime_support=False, rust_adoption=False, runtime_qualified=False, hypothetical=True)

class PoststartError(ValueError):
    pass

def require(ok, code):
    if not ok:
        raise PoststartError(code)

def closed(value, keys):
    require(type(value) is dict and set(value) == set(keys), 'closed_fields')

def counter(value):
    require(type(value) is int and 0 <= value <= 9007199254740991, 'integer')

def digest(value):
    require(type(value) is str and re.fullmatch('[a-f0-9]{64}', value) is not None, 'digest')

def identifier(value):
    try:
        require(type(value) is str and str(uuid.UUID(value)) == value, 'uuid')
    except (ValueError, AttributeError):
        raise PoststartError('uuid') from None

def bounded(value, depth=0):
    require(depth <= 16, 'depth')
    if type(value) is str:
        try:
            size = len(value.encode('utf-8', errors='strict'))
        except UnicodeError:
            raise PoststartError('unicode') from None
        require(0 < size <= 4096, 'string_bound')
    elif type(value) in (dict, list):
        require(len(value) <= 100, 'collection_bound')
        if type(value) is dict:
            for key, item in value.items():
                require(type(key) is str, 'key')
                bounded(key, depth + 1)
                bounded(item, depth + 1)
        else:
            for item in value:
                bounded(item, depth + 1)
    elif type(value) in (int, float):
        require(abs(value) <= 9007199254740991 and math.isfinite(value), 'number')
    else:
        require(value is None or type(value) is bool, 'scalar')

def canonical(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + '\n').encode('utf-8')

def document_digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()

def decode(value, limit=MAX_DOCUMENT):
    raw_input = isinstance(value, (str, bytes))
    if isinstance(value, (str, bytes)):
        try:
            raw = value.encode('utf-8') if isinstance(value, str) else value
        except UnicodeError:
            raise PoststartError('unicode') from None
        require(len(raw) <= limit, 'document_bound')
        def pairs(items):
            result = {}
            for key, item in items:
                require(key not in result, 'duplicate_key')
                result[key] = item
            return result
        try:
            value = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_constant=lambda _: require(False, 'number'))
        except (UnicodeError, ValueError, RecursionError):
            raise PoststartError('json') from None
    bounded(value)
    if not raw_input:
        require(len(canonical(value)) <= limit, 'document_bound')
    return copy.deepcopy(value)

def validate_identity(value):
    closed(value, REFS | REVISIONS | DIGESTS | {'launch_id'})
    bounded(value)
    for key in REFS:
        require(type(value[key]) is str, 'source_reference')
    for key in REVISIONS:
        counter(value[key])
    for key in DIGESTS:
        digest(value[key])
    identifier(value['launch_id'])

def validate_verifier(value):
    closed(value, {'name', 'version', 'digest'})
    bounded(value)
    require(type(value['name']) is str and type(value['version']) is str, 'verifier')
    digest(value['digest'])

def validate_profile(value):
    closed(value, {'name', 'version', 'digest', 'expected_time_budgets'})
    validate_verifier({k: value[k] for k in ('name', 'version', 'digest')})
    closed(value['expected_time_budgets'], {'stop', 'grace', 'execution', 'background'})
    for item in value['expected_time_budgets'].values():
        require(type(item) in (int, float) and math.isfinite(item) and item > 0, 'time_budget')

def validate_document(value, kind=None):
    value = decode(value)
    require(type(value) is dict, 'document_object')
    kind = kind or value.get('kind')
    require(type(kind) is str, 'kind')
    extras = {'contain_request': {'scope', 'profile', 'reason', 'request_id'}, 'replacement_request': {'request_id', 'contain_request_id', 'certificate_digest', 'effect_verifier', 'expected_remaining_attempts'}, 'certificate': {'contain_request_id', 'proofs', 'inventory', 'effect_verifier'}}
    require(kind in extras, 'kind')
    closed(value, set(COMMON) | {'kind', 'ledger_epoch', 'identity'} | extras[kind])
    require(value['kind'] == kind, 'kind')
    for key, expected in COMMON.items():
        require(type(value[key]) is type(expected) and value[key] == expected, 'envelope')
    counter(value['ledger_epoch'])
    validate_identity(value['identity'])
    for key in ('request_id', 'contain_request_id'):
        if key in value:
            identifier(value[key])
    if kind == 'contain_request':
        require(value['scope'] == 'original_launch_complete_owned_closure', 'scope')
        validate_profile(value['profile'])
        require(value['profile']['digest'] == value['identity']['profile_digest'], 'profile_binding')
        require(type(value['reason']) is str and bool(value['reason'].strip()), 'reason')
    else:
        validate_verifier(value['effect_verifier'])
        require(value['effect_verifier']['digest'] == value['identity']['verifier_digest'], 'verifier_binding')
    if kind == 'replacement_request':
        counter(value['expected_remaining_attempts'])
        digest(value['certificate_digest'])
        require(value['request_id'] != value['contain_request_id'], 'distinct_request')
    if kind == 'certificate':
        closed(value['proofs'], PROOFS)
        require(all(type(x) is bool for x in value['proofs'].values()), 'proof_type')
        require(type(value['inventory']) is list and 1 <= len(value['inventory']) <= 100, 'inventory')
        actors = set()
        for item in value['inventory']:
            closed(item, {'actor', 'generation', 'obligation', 'settlement'})
            require(type(item['actor']) is str and type(item['generation']) is str and item['actor'] not in actors, 'actor')
            actors.add(item['actor'])
            require(item['obligation'] in ('required', 'incidental', 'unknown') and item['settlement'] in ('satisfied', 'failed', 'pending', 'unknown'), 'obligation')
        raw = json.dumps(value['inventory'], sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
        require(hashlib.sha256(raw).hexdigest() == value['identity']['closure_digest'], 'closure_binding')
    return value

def evidence_complete(value):
    return all(value['proofs'].values()) and all(x['obligation'] != 'unknown' and x['settlement'] in ('satisfied', 'failed') for x in value['inventory'])


def control_identifier(enrollment, request, label):
    """Deterministic private lineage scoped to enrollment and accepted request."""
    binding = document_digest({'enrollment': enrollment, 'request': request})
    namespace = uuid.uuid5(uuid.NAMESPACE_URL, 'harflow.poststart.synthetic:' + binding)
    return str(uuid.uuid5(namespace, label))
