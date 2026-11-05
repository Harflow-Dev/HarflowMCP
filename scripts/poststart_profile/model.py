"""Pure deterministic durable-boundary fault model; journals are not OS effects."""
import copy
import hashlib
import json
from .validation import Rejected, PROOFS, closed, text

EVENTS = {
 'admit': ('request_id','epoch','payload','authority','capacity','result_accepted','enrolled','single_provider','scope_exact','qualified_profile','qualified_budgets','identity'),
 'prepare_stop': (), 'deliver_stop': (), 'stop_ack': (), 'observe': ('proofs','inventory_complete','obligations_settled'),
 'certify': (), 'release': (),
 'replace': ('request_id','epoch','payload','authority','effect_safe','remaining_attempts','identity'),
 'prepare_dispatch': ('authority','effect_safe','budget','configuration','claims'), 'deliver_dispatch': (), 'dispatch_ack': (),
 'context_change': ('authority','effect_safe','budget','configuration','claims'), 'crash': (), 'recover': (), 'old_callback': (), 'accept_result': (),
 'sibling_result': ('id',), 'capacity_pressure': (),
}
BOOLS={'authority','capacity','result_accepted','enrolled','single_provider','scope_exact',
       'qualified_profile','qualified_budgets','inventory_complete','obligations_settled','effect_safe','budget','configuration','claims'}
def validate_events(events):
    if type(events) is not list or len(events)>100: raise Rejected('event_limit')
    try:
        encoded_size=len(json.dumps(events,ensure_ascii=False,allow_nan=False).encode('utf-8'))
    except (UnicodeError,ValueError,TypeError,RecursionError):
        raise Rejected('invalid_json') from None
    if encoded_size>131072: raise Rejected('document_limit')
    for event in events:
        if type(event) is not dict or type(event.get('kind')) is not str or event['kind'] not in EVENTS: raise Rejected('unknown_event')
        fields=EVENTS[event['kind']]
        closed(event,{'kind',*fields})
        for key in fields:
            value=event[key]
            if key in BOOLS and type(value) is not bool: raise Rejected('event_type')
            elif key=='proofs':
                closed(value,PROOFS)
                if any(type(v) is not bool for v in value.values()): raise Rejected('event_type')
            elif key=='remaining_attempts':
                if type(value) is not int or not 0<=value<=9007199254740991: raise Rejected('event_type')
            elif key not in BOOLS: text(value)

def run_scenario(events, *, initial=None):
    validate_events(events)
    if initial is not None: raise Rejected('initial_state_unsupported')
    s={'runtime_authorized':False,'candidate_only':True,'phase':'running','journal':[], 'effects':{'stop':0,'dispatch':0},
       'intent':None,'replacement':None,'stop_status':'none','dispatch_status':'none',
       'fenced':False,'certificate':False,'released':False,'accepted_result':False,
       'siblings':[], 'original_attempt_charged':True,'remaining_attempts':None,
       'evidence_revision':0,'certificate_revision':None,'model_decisions':0,'old_callback_mutations':0,'verdicts':[]}
    context=dict.fromkeys(('authority','effect_safe','budget','configuration','claims'),True)
    handoff={'stop':False,'dispatch':False}
    def reject(code): s['verdicts'].append(code)
    for e in events:
        k=e['kind']
        if k=='admit':
            binding={key:e[key] for key in ('request_id','epoch','payload','authority','identity','enrolled','single_provider','scope_exact','qualified_profile','qualified_budgets')}
            if s['intent'] is not None:
                reject('accepted_replay' if s['intent']==binding else 'stale_or_conflicting'); continue
            if s['accepted_result'] or e['result_accepted']: reject('accepted_result_wins'); continue
            if not context['authority'] or not e['authority']: reject('unauthorized'); continue
            if not all(e[key] for key in ('enrolled','single_provider','scope_exact','qualified_profile','qualified_budgets')): reject('unsupported_profile'); continue
            if not e['capacity']: reject('backpressure'); continue
            s.update(intent=binding,fenced=True,phase='cancel_requested',original_identity=e['identity'])
            s['journal'].extend(['intent','future_issuance_and_acceptance_fence','completion_space_reserved'])
        elif k=='prepare_stop':
            if not s['fenced']: reject('no_intent'); continue
            if s['stop_status']!='none': reject('stop_not_resent'); continue
            s['journal'].append('stop_delivery_intent'); s['stop_status']='unknown'
            handoff['stop']=True; s['phase']='settling'
        elif k=='deliver_stop':
            if not handoff['stop']: reject('stop_delivery_unknown'); continue
            handoff['stop']=False; s['effects']['stop']+=1
        elif k=='stop_ack':
            if s['stop_status']!='unknown' or not s['effects']['stop']: reject('no_stop_delivery'); continue
            s['stop_status']='acknowledged'; s['journal'].append('stop_ack')
        elif k=='observe':
            if not s['fenced'] or s['stop_status']=='none': reject('no_stop_delivery'); continue
            handoff={'stop':False,'dispatch':False}
            s['evidence_revision']+=1
            s['proofs']=copy.deepcopy(e['proofs'])
            s['certificate']=False; s['certificate_revision']=None; s['released']=False
            s['complete']=e['inventory_complete'] and e['obligations_settled'] and all(e['proofs'].values())
            s['phase']='settling' if s['complete'] else 'settlement_unknown'
            s['journal'].append('bounded_observation')
        elif k=='certify':
            if not s.get('complete',False): reject('settlement_unknown'); continue
            s['certificate']=True; s['certificate_revision']=s['evidence_revision']; s['phase']='ready_to_replan'; s['journal'].append('certificate')
        elif k=='release':
            if not s['certificate']: reject('certificate_required'); continue
            s['released']=True; s['journal'].extend(['old_claims_release','coordinator_exclusion_hold'])
        elif k=='replace':
            binding={key:e[key] for key in ('request_id','epoch','payload','authority','identity','effect_safe','remaining_attempts')}
            if s['replacement'] is not None:
                reject('accepted_replay' if s['replacement']==binding else 'stale_or_conflicting'); continue
            if not s['certificate'] or not s['released']: reject('certificate_required'); continue
            if e['request_id']==s['intent']['request_id']: reject('request_identity_reused'); continue
            if e['epoch']!=s['intent']['epoch']: reject('stale_or_conflicting'); continue
            if e['identity']!=s['original_identity']: reject('immutable_assignment_mismatch'); continue
            if not context['authority'] or not e['authority']: reject('unauthorized'); continue
            if not all(context.values()): reject('link_revalidation_failed'); continue
            if not e['effect_safe']: reject('effects_not_repeat_safe'); continue
            if e['remaining_attempts']<=0: reject('budget_exhausted'); continue
            s['replacement']=binding
            token=hashlib.sha256(json.dumps(binding,sort_keys=True,separators=(',',':')).encode()).hexdigest()
            s['lineage']={'supersedes':s['intent']['request_id'],'decision_id':'control-'+token,'attempt_id':'attempt-'+token,'launch_id':'launch-'+token,'identity':e['identity']}
            s['remaining_attempts']=e['remaining_attempts']-1
            s['journal'].append('atomic_control_attempt_lineage_launch'); s['phase']='replacement_linked'
        elif k=='prepare_dispatch':
            if s['replacement'] is None: reject('replacement_required'); continue
            if s['dispatch_status']!='none': reject('launch_identity_retained'); continue
            if not s['certificate'] or s['certificate_revision']!=s['evidence_revision'] or not all(context.values()) or not all(e[key] for key in ('authority','effect_safe','budget','configuration','claims')):
                s['phase']='needs_attention'; reject('dispatch_revalidation_failed'); continue
            s['journal'].extend(['exclusion_handoff','dispatch_intent']); s['dispatch_status']='unknown'
            handoff['dispatch']=True; s['phase']='launch_unknown'
        elif k=='deliver_dispatch':
            if not handoff['dispatch']: reject('launch_delivery_unknown'); continue
            handoff['dispatch']=False; s['effects']['dispatch']+=1
        elif k=='dispatch_ack':
            if s['dispatch_status']!='unknown' or not s['effects']['dispatch']: reject('no_dispatch'); continue
            s['dispatch_status']='acknowledged'; s['phase']='replacement_running'; s['journal'].append('dispatch_ack')
        elif k=='context_change':
            context={key:e[key] for key in context}
            handoff={'stop':False,'dispatch':False}
            if not all(context.values()): s['phase']='needs_attention'
        elif k in ('crash','recover'):
            handoff={'stop':False,'dispatch':False}; s['journal'].append(k)
        elif k=='old_callback': reject('old_callback_fenced' if s['fenced'] else 'no_successor')
        elif k=='accept_result':
            if s['fenced']: reject('old_callback_fenced')
            else: s['accepted_result']=True
        elif k=='sibling_result':
            if e['id'] not in s['siblings']: s['siblings'].append(e['id'])
        elif k=='capacity_pressure': s['journal'].append('admission_backpressure_observation_and_replay_available')
    return s
