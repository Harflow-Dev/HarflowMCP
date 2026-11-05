"""Bounded closed JSON validation for candidate documents only."""
import json
import math
import re

HEADER = {'format':'harflow.steering.poststart.candidate','v':1,'status': 'specified_unimplemented', 'provenance': 'synthetic_specification',
          'python_runtime_support': False, 'rust_adoption': False, 'runtime_qualified': False}
LIMIT = 131072
PROOFS = ('qualified_profile', 'no_pending_mutation', 'sessions_reconciled', 'resources_reconciled', 'messages_reconciled', 'original_enrollment', 'complete_closure', 'future_issuance_fence', 'exit_reaped',
          'stdout_eof', 'stderr_eof', 'input_reconciled', 'lossless_capture', 'final_watermark',
          'writers_quiescent', 'extents_reconciled', 'handles_settled', 'claims_settled',
          'release_journal_complete', 'job_quiescent', 'stale_callback_fence', 'all_obligations_settled')
IDENTITY = ('root', 'activation', 'task', 'attempt', 'visit',
            'definition_digest', 'goal_digest', 'goal_revision', 'interaction_revision', 'control_revision', 'target_revision',
            'assignment_digest', 'configuration_digest', 'grant_digest', 'owner_generation',
            'containment_generation', 'process_generation', 'launch_id', 'profile_digest', 'verifier_digest', 'closure_digest', 'node', 'candidate')
REVISIONS={'goal_revision','interaction_revision','control_revision','target_revision'}
DIGESTS={key for key in IDENTITY if key.endswith('_digest')}
UUID_PATTERN=r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
SHA_PATTERN=r'[0-9a-f]{64}'
class Rejected(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)

def closed(value, fields):
    if type(value) is not dict or set(value) != set(fields):
        raise Rejected('closed_shape')

def text(value):
    if type(value) is not str or not value:
        raise Rejected('invalid_string')
    try:
        size=len(value.encode('utf-8'))
    except UnicodeError:
        raise Rejected('invalid_string') from None
    if size>4096:
        raise Rejected('invalid_string')

def walk(value, depth=0):
    if depth > 16:
        raise Rejected('depth_limit')
    if isinstance(value, dict):
        if len(value) > 100:
            raise Rejected('inventory_limit')
        for k,v in value.items():
            text(k); walk(v,depth+1)
    elif isinstance(value,list):
        if len(value)>100: raise Rejected('inventory_limit')
        for v in value: walk(v,depth+1)
    elif isinstance(value,str): text(value)
    elif value is not None and type(value) not in (bool,int,float): raise Rejected('json_type')
    elif type(value) in (int,float) and (abs(value)>9007199254740991 or not math.isfinite(value)):
        raise Rejected('number_limit')

def pairs(items):
    result={}
    for k,v in items:
        if k in result: raise Rejected('duplicate_key')
        result[k]=v
    return result

def decode(raw):
    if not isinstance(raw,(bytes,str)): raise Rejected('raw_type')
    try:
        data=raw.encode('utf-8') if isinstance(raw,str) else raw
        if len(data)>LIMIT: raise Rejected('document_limit')
        doc=json.loads(data.decode('utf-8'),object_pairs_hook=pairs,
                       parse_constant=lambda _: (_ for _ in ()).throw(Rejected('number_limit')))
    except Rejected: raise
    except (UnicodeError,ValueError,RecursionError,OverflowError): raise Rejected('invalid_json') from None
    walk(doc)
    if type(doc) is not dict: raise Rejected('closed_shape')
    kind=doc.get('kind')
    common=set(HEADER)|{'kind','hypothetical'}
    shapes={'contain_request': {'request_id','ledger_epoch','identity','scope','profile','reason'},
            'replacement_request': {'request_id','contain_request_id','ledger_epoch','identity','certificate_digest','effect_verifier','expected_remaining_attempts'},
            'certificate': {'contain_request_id','ledger_epoch','identity','proofs','inventory','effect_verifier'},
            'scenario': {'events'}}
    if type(kind) is not str or kind not in shapes: raise Rejected('unknown_kind')
    closed(doc,common|shapes[kind])
    for k,v in HEADER.items():
        if type(doc[k]) is not type(v) or doc[k]!=v: raise Rejected('candidate_only')
    if doc['hypothetical'] is not True: raise Rejected('candidate_only')
    if kind=='scenario':
        from .model import validate_events
        validate_events(doc['events'])
        return doc
    for key in ('request_id','contain_request_id'):
        if key in doc and (type(doc[key]) is not str or re.fullmatch(UUID_PATTERN,doc[key]) is None): raise Rejected('invalid_request_id')
    if type(doc['ledger_epoch']) is not int or not 0<=doc['ledger_epoch']<=9007199254740991: raise Rejected('invalid_revision')
    closed(doc['identity'],IDENTITY)
    for key,value in doc['identity'].items():
        if key in REVISIONS:
            if type(value) is not int or not 0<=value<=9007199254740991: raise Rejected('invalid_revision')
        else:
            text(value)
            pattern=SHA_PATTERN if key in DIGESTS else UUID_PATTERN if key in ('request_id','launch_id') else None
            if pattern and re.fullmatch(pattern,value) is None: raise Rejected('invalid_identity')
    if kind=='contain_request':
        if doc['scope']!='original_launch_complete_owned_closure': raise Rejected('unsupported_scope')
        closed(doc['profile'],('name','version','digest','expected_time_budgets'))
        text(doc['profile']['name']); text(doc['profile']['version'])
        if type(doc['profile']['digest']) is not str or re.fullmatch(SHA_PATTERN,doc['profile']['digest']) is None: raise Rejected('invalid_digest')
        if doc['profile']['digest']!=doc['identity']['profile_digest']: raise Rejected('profile_mismatch')
        closed(doc['profile']['expected_time_budgets'],('stop','grace','execution','background'))
        for v in doc['profile']['expected_time_budgets'].values():
            if type(v) not in (int,float) or v<=0: raise Rejected('unqualified_budgets')
        text(doc['reason'])
        if not doc['reason'].strip(): raise Rejected('blank_reason')
    elif kind=='certificate':
        closed(doc['proofs'],PROOFS)
        if any(type(v) is not bool for v in doc['proofs'].values()): raise Rejected('closed_shape')
        if type(doc['inventory']) is not list or not doc['inventory']: raise Rejected('incomplete_inventory')
        seen=set()
        for item in doc['inventory']:
            closed(item,('actor','generation','obligation','settlement'))
            for v in item.values(): text(v)
            if item['obligation'] not in ('required','incidental','unknown'): raise Rejected('invalid_obligation')
            if item['actor'] in seen: raise Rejected('duplicate_actor')
            seen.add(item['actor'])
            if item['settlement'] not in ('satisfied','failed','pending','unknown'): raise Rejected('closed_shape')
        import hashlib
        inventory_digest=hashlib.sha256(json.dumps(doc['inventory'],sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
        if inventory_digest!=doc['identity']['closure_digest']: raise Rejected('inventory_mismatch')
        validate_verifier(doc['effect_verifier'])
        if doc['effect_verifier']['digest']!=doc['identity']['verifier_digest']: raise Rejected('verifier_mismatch')
    else:
        text(doc['certificate_digest'])
        if re.fullmatch(SHA_PATTERN,doc['certificate_digest']) is None: raise Rejected('invalid_digest')
        validate_verifier(doc['effect_verifier'])
        if doc['effect_verifier']['digest']!=doc['identity']['verifier_digest']: raise Rejected('verifier_mismatch')
        if type(doc['expected_remaining_attempts']) is not int or doc['expected_remaining_attempts']<0: raise Rejected('invalid_budget')
    return doc

def validate_verifier(value):
    closed(value,('name','version','digest'))
    for field in value.values(): text(field)
    if re.fullmatch(SHA_PATTERN,value['digest']) is None: raise Rejected('invalid_digest')

def classify(raw):
    try:
        doc=decode(raw)
        if doc['kind']=='certificate':
            if not all(doc['proofs'].values()) or any(x['settlement'] not in ('satisfied','failed') or x['obligation']=='unknown' for x in doc['inventory']):
                raise Rejected('settlement_unknown')
        return {'candidate_valid': True,'code':'structural_valid_untrusted' if doc['kind'] in ('contain_request','replacement_request') else 'hypothetical_valid','runtime_authorized':False}
    except Rejected as exc: return {'candidate_valid':False,'code':exc.code,'runtime_authorized':False}

def validate_link(contain_raw, certificate_raw, replacement_raw):
    """Validate synthetic cross-document lineage; never authenticates a certificate."""
    import hashlib
    contain,certificate,replacement=(decode(raw) for raw in (contain_raw,certificate_raw,replacement_raw))
    if [doc['kind'] for doc in (contain,certificate,replacement)] != ['contain_request','certificate','replacement_request']:
        raise Rejected('link_kinds')
    if not contain['identity']==certificate['identity']==replacement['identity']:
        raise Rejected('source_identity_mismatch')
    if not contain['ledger_epoch']==certificate['ledger_epoch']==replacement['ledger_epoch']:
        raise Rejected('ledger_epoch_mismatch')
    if not contain['request_id']==certificate['contain_request_id']==replacement['contain_request_id']:
        raise Rejected('contain_reference_mismatch')
    if replacement['request_id']==contain['request_id']:
        raise Rejected('request_identity_reused')
    raw=(json.dumps(certificate,sort_keys=True,indent=2,ensure_ascii=False,allow_nan=False)+'\n').encode('utf-8')
    if replacement['certificate_digest']!=hashlib.sha256(raw).hexdigest():
        raise Rejected('certificate_digest_mismatch')
    return {'candidate_valid':True,'code':'structural_link_valid_untrusted','runtime_authorized':False}
