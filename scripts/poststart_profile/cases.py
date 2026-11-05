"""Hypothetical fixtures: never production tokens or ctl envelopes."""
import json
import copy
import hashlib
from .validation import HEADER, IDENTITY, PROOFS, REVISIONS, DIGESTS

def canonical(value):
    return (json.dumps(value,sort_keys=True,indent=2,ensure_ascii=False,allow_nan=False)+'\n').encode()

def documents():
    identity={key:0 if key in REVISIONS else 'a'*64 if key in DIGESTS else '00000000-0000-4000-8000-000000000001' if key in ('request_id','launch_id') else 'synthetic-'+key for key in IDENTITY}
    verifier={'name':'synthetic-enforced-absence','version':'1','digest':'b'*64}
    identity['verifier_digest']=verifier['digest']
    inventory=[{'actor':'original','generation':'synthetic-1','obligation':'incidental','settlement':'satisfied'}]
    identity['closure_digest']=hashlib.sha256(json.dumps(inventory,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    request_id='00000000-0000-4000-8000-000000000002'
    base={**HEADER,'hypothetical':True}
    contain={**base,'kind':'contain_request','request_id':request_id,'ledger_epoch':1,'identity':copy.deepcopy(identity),
             'scope':'original_launch_complete_owned_closure','reason':'synthetic containment',
             'profile':{'name':'synthetic-only','version':'1','digest':identity['profile_digest'],
                        'expected_time_budgets':{'stop':1,'grace':1,'execution':1,'background':1}}}
    certificate={**base,'kind':'certificate','contain_request_id':request_id,'ledger_epoch':1,'identity':copy.deepcopy(identity),'proofs':dict.fromkeys(PROOFS,True),
                 'inventory':inventory,
                 'effect_verifier':verifier}
    replacement={**base,'kind':'replacement_request','request_id':'00000000-0000-4000-8000-000000000003','contain_request_id':request_id,'ledger_epoch':1,'identity':copy.deepcopy(identity),'certificate_digest':hashlib.sha256(canonical(certificate)).hexdigest(),
                 'effect_verifier':verifier,'expected_remaining_attempts':1}
    admit={'kind':'admit','request_id':'contain-1','epoch':'epoch-1','payload':'payload-1','authority':True,
           'capacity':True,'result_accepted':False,'enrolled':True,'single_provider':True,'scope_exact':True,'qualified_profile':True,'qualified_budgets':True,'identity':'immutable-1'}
    events=[admit,{'kind':'prepare_stop'},{'kind':'deliver_stop'},{'kind':'stop_ack'},
            {'kind':'observe','proofs':dict.fromkeys(PROOFS,True),'inventory_complete':True,'obligations_settled':True},
            {'kind':'certify'},{'kind':'release'},
            {'kind':'replace','request_id':'replace-1','epoch':'epoch-1','payload':'payload-2','authority':True,
             'effect_safe':True,'remaining_attempts':1,'identity':'immutable-1'},
            {'kind':'prepare_dispatch','authority':True,'effect_safe':True,'budget':True,'configuration':True,'claims':True},
            {'kind':'deliver_dispatch'},{'kind':'dispatch_ack'},{'kind':'old_callback'}]
    result={'contain-request.json':contain,'certificate.json':certificate,'replacement-request.json':replacement,
            'hypothetical-sequence.json':{**base,'kind':'scenario','events':events}}
    reused=copy.deepcopy(events)
    next(event for event in reused if event['kind']=='replace')['request_id']=admit['request_id']
    result['reused-action-request-scenario.json']={**base,'kind':'scenario','events':reused}
    for proof in PROOFS:
        case=copy.deepcopy(certificate); case['proofs'][proof]=False
        result['missing-'+proof.replace('_','-')+'.json']=case
    for boundary in range(len(events)+1):
        fault=copy.deepcopy(events[:boundary])+[{'kind':'crash'},{'kind':'recover'}]+copy.deepcopy(events[boundary:])
        result['crash-cut-'+str(boundary)+'.json']={**base,'kind':'scenario','events':fault}
    pending=copy.deepcopy(certificate); pending['inventory'][0]['settlement']='pending'
    pending['identity']['closure_digest']=hashlib.sha256(json.dumps(pending['inventory'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
    result['incidental-pending.json']=pending
    unknown=copy.deepcopy(certificate); unknown['inventory'][0]['obligation']='unknown'
    unknown['identity']['closure_digest']=hashlib.sha256(json.dumps(unknown['inventory'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
    result['unknown-obligation-settled.json']=unknown
    case=copy.deepcopy(contain); case['v']=1.0
    result['reject-float-version.json']=case
    case=copy.deepcopy(contain); case['ledger_epoch']=1.0
    result['reject-float-revision.json']=case
    case=copy.deepcopy(contain); case['reason']='é'*2049
    result['reject-utf8-string-budget.json']=case
    case=copy.deepcopy(certificate); case['inventory'].append(copy.deepcopy(case['inventory'][0]))
    case['identity']['closure_digest']=hashlib.sha256(json.dumps(case['inventory'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
    result['reject-duplicate-actor.json']=case
    case=copy.deepcopy(contain); case['python_runtime_support']=True
    result['reject-runtime-header.json']=case
    case=copy.deepcopy(contain); case['authority']=True
    result['reject-caller-authority.json']=case
    case=copy.deepcopy(replacement); case['effect_verifier']['digest']='d'*64
    result['reject-verifier-binding.json']=case
    case=copy.deepcopy(contain); case['profile']['digest']='e'*64
    result['reject-profile-binding.json']=case
    case=copy.deepcopy(contain); case['scope']='entire_root'
    result['reject-scope.json']=case
    case=copy.deepcopy(replacement); case['task_updates']=[]
    result['reject-ordinary-frame.json']=case
    case=copy.deepcopy(replacement); case['contain_request_id']='invalid-reference'
    result['reject-reference-syntax.json']=case
    case=copy.deepcopy(replacement); case['expected_remaining_attempts']=0
    result['zero-budget-untrusted-witness.json']=case
    return result

def fixtures():
    from .validation import classify
    from .model import run_scenario
    return {name:(canonical(doc),{'classification':classify(canonical(doc)),
             'model':run_scenario(doc['events']) if doc['kind']=='scenario' else None})
            for name,doc in documents().items()}
