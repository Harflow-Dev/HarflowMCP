"""Pure conversation transitions; callers serialize them with their authoritative CAS.

These are internal engine ports, not public authority assertions. Native evidence
must be correlated by the owning adapter before record_receipt is invoked.
"""
from __future__ import annotations
from copy import deepcopy
from .conversation_documents import (CONTRACT, ConversationError, MAX_MESSAGE_BYTES,
    MAX_OUTSTANDING, MAX_HISTORY, MAX_DOCUMENT_BYTES, COMPLETION_RESERVE_BYTES, MAX_TEXT_BYTES,
    encoded, validate, identifier, request_uuid, utf8_size)

def new_state(conversation_id, head_task_id, native_context_id, generation, turn_id):
    return validate(dict(contract=CONTRACT, conversation_id=conversation_id,
        head_task_id=head_task_id, native_context_id=native_context_id,
        generation=generation, turn_id=turn_id, boundary=False, closed=False,
        sequence=0, messages=[], ledger_epoch=0, cumulative_issuances=0, runtime_fault=None,
        native_progress=dict(generation=generation,logical_turn=turn_id,phase='starting',native_terminal=None,
                             pending_tools=0,pending_approvals=0,unsettled_effects=0),
        accounting=dict(settled_responses=0,accounted_generation=-1,observed_cost_usd=None,
                        usage_complete=False,cost_basis='unavailable',last_cost_usd=None,last_cost_basis='unavailable')))

def _copy(state):
    return deepcopy(validate(state))

def _item(state, request_id):
    return next((m for m in state['messages'] if m['request_id'] == request_id), None)

def _finish(state):
    # Unknown and issued claims are protected indefinitely, never expired.
    finals = [m for m in state['messages'] if m['status'] in ('cancelled','consumed')]
    discard = {m['request_id'] for m in finals[:-MAX_HISTORY]}
    text_bytes = sum(utf8_size(m['text']) for m in state['messages'] if m['request_id'] not in discard)
    for item in finals:
        if text_bytes <= MAX_TEXT_BYTES:
            break
        if item['request_id'] not in discard:
            discard.add(item['request_id'])
            text_bytes -= utf8_size(item['text'])
    if discard: state['ledger_epoch'] += 1
    state['messages'] = [m for m in state['messages'] if m['request_id'] not in discard]
    return validate(state)

def admit(state, *, request_id, text, delivery, target_task_id, expected_generation=None, expected_turn_id=None, expected_epoch=None, original_target_task_id=None):
    state = _copy(state); request_uuid(request_id)
    if any((m['promotion'] and m['promotion']['request_id'] == request_id)
            or (m['cancellation'] and m['cancellation']['request_id'] == request_id) for m in state['messages']):
        raise ConversationError('request_conflict')
    if original_target_task_id is None: original_target_task_id = target_task_id
    identifier(original_target_task_id)
    old = _item(state, request_id)
    if old:
        if (old['text'],old['delivery'],old['original_target_task_id']) != (text,delivery,original_target_task_id):
            raise ConversationError('request_conflict')
        if ((expected_generation is not None and expected_generation != old['source_generation'])
                or (expected_turn_id is not None and expected_turn_id != old['source_turn_id'])
                or (expected_epoch is not None and expected_epoch != old['source_epoch'])):
            raise ConversationError('request_conflict')
        return state, deepcopy(old)
    if type(expected_epoch) is not int or expected_epoch != state['ledger_epoch']:
        raise ConversationError('request_expired')
    if state['runtime_fault'] is not None: raise ConversationError('conversation_unknown')
    if not isinstance(text,str) or not text or utf8_size(text) > MAX_MESSAGE_BYTES or delivery not in ('queue','now'):
        raise ConversationError('invalid_request')
    identifier(target_task_id)
    if delivery=='now' and any(state['native_progress'][key] for key in ('pending_tools','pending_approvals','unsettled_effects')):
        raise ConversationError('tool_interruption_unqualified')
    if state['closed'] or target_task_id != state['head_task_id'] or (expected_generation is not None and expected_generation != state['generation']) or (expected_turn_id is not None and expected_turn_id != state['turn_id']):
        raise ConversationError('target_stale')
    outstanding = [m for m in state['messages'] if m['status'] in ('queued','claimed','unknown')]
    if len(outstanding) >= MAX_OUTSTANDING:
        raise ConversationError('queue_full')
    if delivery == 'now' and _now_busy(state):
        raise ConversationError('now_busy')
    state['sequence'] += 1
    item = dict(request_id=request_id,text=text,delivery=delivery,target_task_id=target_task_id,original_target_task_id=original_target_task_id,
        source_generation=state['generation'],source_turn_id=state['turn_id'],source_epoch=state['ledger_epoch'],
        sequence=state['sequence'],status='queued',claim=None,receipt=None,promotion=None,interrupt_intent=None,cancellation=None)
    state['messages'].append(item)
    state = _finish(state)
    if len(encoded(state)) > MAX_DOCUMENT_BYTES - COMPLETION_RESERVE_BYTES:
        raise ConversationError('queue_full')
    return state, deepcopy(item)

def cancel(state, request_id):
    state = _copy(state); item = _item(state,request_uuid(request_id))
    if item is None: raise ConversationError('request_missing')
    if item['status'] == 'cancelled': return state
    if item['status'] != 'queued': raise ConversationError('already_claimed')
    if item['interrupt_intent'] is not None: raise ConversationError('interrupt_already_issued')
    item['status'] = 'cancelled'
    return _finish(state)

def observe_boundary(state, *, generation, turn_id):
    state = _copy(state)
    if (generation,turn_id) != (state['generation'],state['turn_id']):
        raise ConversationError('turn_mismatch')
    state['boundary'] = True
    state['native_progress']['phase']='unknown' if state['runtime_fault'] is not None else 'boundary'
    return state

def claim_next(state, *, claim_id, destination_task_id=None, destination_turn_id=None, destination_generation=None):
    state = _copy(state); request_uuid(claim_id)
    previous = next((m for m in state['messages'] if m['claim'] and m['claim']['claim_id'] == claim_id),None)
    if previous:
        claim = previous['claim']
        if any(value is not None and value != claim[key] for key,value in (
            ('destination_task_id',destination_task_id),('destination_turn_id',destination_turn_id),
            ('destination_generation',destination_generation))):
            raise ConversationError('claim_conflict')
        return state, deepcopy(previous)
    if state['runtime_fault'] is not None or state['closed'] or state['native_context_id'] is None or not state['boundary'] or any(m['status'] in ('claimed','unknown') for m in state['messages']):
        return state, None
    queued = [m for m in state['messages'] if m['status'] == 'queued']
    if not queued: return state, None
    destination_task_id = identifier(state['head_task_id'] if destination_task_id is None else destination_task_id)
    destination_turn_id = identifier(claim_id if destination_turn_id is None else destination_turn_id)
    if destination_generation is None: destination_generation = state['generation'] + 1
    if type(destination_generation) is not int or destination_generation != state['generation'] + 1:
        raise ConversationError('destination_mismatch')
    if destination_turn_id == state['turn_id']:
        raise ConversationError('destination_mismatch')
    item = min(queued,key=lambda m:(not _priority(m),m['sequence']))
    state['cumulative_issuances'] += 1
    item['status'] = 'claimed'
    item['claim'] = dict(claim_id=claim_id,origin='now_delivery' if _priority(item) else 'queue_drain',conversation_id=state['conversation_id'],
        native_context_id=state['native_context_id'],source_task_id=state['head_task_id'],
        source_generation=state['generation'],source_turn_id=state['turn_id'],request_id=item['request_id'],
        destination_task_id=destination_task_id,destination_turn_id=destination_turn_id,
        destination_generation=destination_generation)
    state.update(head_task_id=destination_task_id,generation=destination_generation,
                 turn_id=destination_turn_id,boundary=False)
    state['native_progress']=dict(generation=destination_generation,logical_turn=destination_turn_id,phase='issuing',
        native_terminal=None,pending_tools=0,pending_approvals=0,unsettled_effects=0)
    return _finish(state), deepcopy(item)

def mark_unknown(state, *, claim_id):
    state = _copy(state)
    item = next((m for m in state['messages'] if m['claim'] and m['claim']['claim_id'] == claim_id),None)
    if item is None: raise ConversationError('claim_missing')
    if item['status'] == 'consumed': return state
    item['status'] = 'unknown'
    state['native_progress']['phase']='unknown'
    state['accounting'].update(usage_complete=False,observed_cost_usd=None,cost_basis='unavailable')
    return _finish(state)

def record_receipt(state, *, receipt, usage_evidence=None):
    state = _copy(state)
    if not isinstance(receipt,dict) or not isinstance(receipt.get('claim'),dict):
        raise ConversationError('receipt_mismatch')
    item = next((m for m in state['messages'] if m['claim'] == receipt['claim']),None)
    if item is None: raise ConversationError('receipt_mismatch')
    if item['status'] == 'consumed':
        if receipt != item['receipt']: raise ConversationError('receipt_conflict')
        return state
    item['receipt'] = deepcopy(receipt); item['status'] = 'consumed'
    validate(state)  # validates exact consumption evidence, never a spawn receipt
    state=record_response(state,generation=receipt['generation'],turn_id=receipt['turn_id'],**(usage_evidence or {}))
    return _finish(state)

def close(state):
    state = _copy(state)
    if state['runtime_fault'] is not None: raise ConversationError('conversation_unknown')
    if any(m['status'] in ('queued','claimed','unknown') for m in state['messages']):
        raise ConversationError('conversation_pending')
    if not state['boundary']: raise ConversationError('turn_active')
    state['closed'] = True
    state['native_progress']['phase']='closed'
    return state


def bind_native_context(state, *, context_id, generation):
    """Trusted owner event binds native identity once, never substitutes Fresh."""
    state = _copy(state); identifier(context_id)
    if generation != state['generation']:
        raise ConversationError('turn_mismatch')
    if state['native_context_id'] not in (None, context_id):
        raise ConversationError('context_conflict')
    state['native_context_id'] = context_id
    return validate(state)


def drive_once(store, deliver, **destination):
    """Reserve durably, invoke an injected trusted effect port outside the lock.

    deliver(claimed_message) returns a correlated native consumption receipt.
    Exceptions mean delivery is uncertain, never released for automatic replay.
    Recovery of a claimed item needs external receipt reconciliation, not this
    driver: it deliberately never reissues an existing claim.
    """
    import uuid
    claim_id = str(uuid.uuid4())
    claimed = store.update(lambda old: claim_next(old, claim_id=claim_id, **destination))
    if claimed is None:
        return None
    try:
        receipt = deliver(deepcopy(claimed))
        return store.update(lambda old: (record_receipt(old, receipt=receipt), receipt))
    except BaseException:
        # If this persistence also fails, the durable claimed state remains a
        # protected uncertain effect. Neither state is eligible for replay.
        try:
            store.update(lambda old: (mark_unknown(old, claim_id=claim_id), None))
        except Exception:
            pass
        raise


def protects_retention(state):
    """Fail closed on corrupt state; unresolved deliveries never expire by age."""
    validate(state)
    return state['runtime_fault'] is not None or any(item['status'] in ('queued', 'claimed', 'unknown') for item in state['messages'])


def _priority(item):
    return item['delivery'] == 'now' or item['promotion'] is not None


def _now_busy(state):
    return any(_priority(m) and m['status'] in ('queued','claimed','unknown')
               and (m['promotion']['generation'] if m['promotion'] else m['source_generation']) == state['generation']
               for m in state['messages'])


def promote_now(state, *, request_id, message_request_id, target_task_id,
                expected_generation=None, expected_turn_id=None, expected_epoch=None):
    """Idempotent control request; original admission fingerprint stays intact."""
    state = _copy(state); request_uuid(request_id); request_uuid(message_request_id)
    identifier(target_task_id)
    previous = next((m['promotion'] for m in state['messages']
                     if m['promotion'] and m['promotion']['request_id'] == request_id), None)
    if previous:
        if (previous['message_request_id'],previous['target_task_id']) != (message_request_id,target_task_id):
            raise ConversationError('request_conflict')
        if ((expected_generation is not None and expected_generation != previous['generation'])
                or (expected_turn_id is not None and expected_turn_id != previous['turn_id'])
                or (expected_epoch is not None and expected_epoch != previous['ledger_epoch'])):
            raise ConversationError('request_conflict')
        return state,deepcopy(previous)
    if type(expected_epoch) is not int or expected_epoch != state['ledger_epoch']:
        raise ConversationError('request_expired')
    if state['runtime_fault'] is not None: raise ConversationError('conversation_unknown')
    if any(state['native_progress'][key] for key in ('pending_tools','pending_approvals','unsettled_effects')):
        raise ConversationError('tool_interruption_unqualified')
    if _item(state,request_id) is not None or any(m['cancellation'] and m['cancellation']['request_id'] == request_id for m in state['messages']):
        raise ConversationError('request_conflict')
    if (state['closed'] or target_task_id != state['head_task_id']
            or (expected_generation is not None and expected_generation != state['generation'])
            or (expected_turn_id is not None and expected_turn_id != state['turn_id'])):
        raise ConversationError('target_stale')
    item = _item(state,message_request_id)
    if item is None: raise ConversationError('request_missing')
    if item['status'] != 'queued': raise ConversationError('already_claimed')
    if item['promotion'] or _now_busy(state): raise ConversationError('now_busy')
    control = dict(request_id=request_id,message_request_id=message_request_id,
                   target_task_id=target_task_id,generation=state['generation'],turn_id=state['turn_id'],ledger_epoch=state['ledger_epoch'])
    item['promotion'] = control
    return _finish(state),deepcopy(control)


def record_response(state, *, generation, turn_id, cost_usd=None, cost_basis='unavailable'):
    """Trusted response accounting; never guess whether a provider cost is local.

    Native cumulative totals are retained as observed totals, never added. An
    unknown response or mixed basis permanently prevents a complete usage claim.
    """
    import math
    state=_copy(state)
    if (generation,turn_id)!=(state['generation'],state['turn_id']):
        raise ConversationError('turn_mismatch')
    if cost_basis not in {'response_local','native_cumulative','unavailable'}:
        raise ConversationError('invalid_usage_evidence')
    if cost_basis=='unavailable':
        if cost_usd is not None: raise ConversationError('invalid_usage_evidence')
    elif type(cost_usd) not in (int,float) or not math.isfinite(cost_usd) or cost_usd<0:
        raise ConversationError('invalid_usage_evidence')
    accounting=state['accounting']
    if accounting['accounted_generation']==generation:
        if (accounting['last_cost_usd'],accounting['last_cost_basis'])!=(cost_usd,cost_basis):
            raise ConversationError('usage_evidence_conflict')
        return state
    first=accounting['settled_responses']==0
    contiguous=(first and generation==0) or generation==accounting['accounted_generation']+1
    compatible=first or accounting['cost_basis']==cost_basis
    known=cost_basis!='unavailable'
    complete=known and contiguous and compatible and (first or accounting['usage_complete'])
    observed=None
    if known and compatible:
        if cost_basis=='native_cumulative': observed=cost_usd
        elif first: observed=cost_usd
        elif accounting['observed_cost_usd'] is not None: observed=accounting['observed_cost_usd']+cost_usd
    accounting.update(settled_responses=accounting['settled_responses']+1,
        accounted_generation=generation,observed_cost_usd=observed,usage_complete=complete,
        cost_basis=cost_basis if compatible else 'unavailable',last_cost_usd=cost_usd,last_cost_basis=cost_basis)
    return validate(state)


def record_interrupt_intent(state, *, request_id, native_identity):
    """Persist exact native interruption correlation before external I/O."""
    state=_copy(state); request_uuid(request_id)
    item=_item(state,request_id)
    if item is None: raise ConversationError('request_missing')
    if not _priority(item) or item['status']!='queued': raise ConversationError('interrupt_not_pending')
    expected={'session_id','turn_id','generation','logical_turn'}
    if not isinstance(native_identity,dict) or set(native_identity)!=expected:
        raise ConversationError('interrupt_identity_mismatch')
    for key in ('session_id','turn_id','logical_turn'): identifier(native_identity[key])
    if (native_identity['session_id']!=state['native_context_id']
            or type(native_identity['generation']) is not int
            or native_identity['generation']!=state['generation']
            or native_identity['logical_turn']!=state['turn_id']):
        raise ConversationError('interrupt_identity_mismatch')
    if item['interrupt_intent'] is not None and item['interrupt_intent']!=native_identity:
        raise ConversationError('interrupt_identity_conflict')
    item['interrupt_intent']=deepcopy(native_identity)
    return validate(state)


def mark_runtime_fault(state, *, code, generation, turn_id):
    """Pin a native owner failure even before any user message was admitted."""
    state=_copy(state); identifier(code)
    if (generation,turn_id)!=(state['generation'],state['turn_id']): raise ConversationError('turn_mismatch')
    fault=dict(code=code,generation=generation,turn_id=turn_id)
    if state['runtime_fault'] is not None and state['runtime_fault']!=fault:
        raise ConversationError('runtime_fault_conflict')
    state['runtime_fault']=fault
    state['native_progress']['phase']='unknown'
    state['accounting'].update(usage_complete=False,observed_cost_usd=None,cost_basis='unavailable')
    return validate(state)


def cancel_control(state, *, request_id, message_request_id, target_task_id,
                   expected_generation=None, expected_turn_id=None, expected_epoch=None):
    """Durable cancellation intent, separate from the original message UUID.

    Retained exact receipts precede source/epoch checks. An evicted old receipt
    cannot be recreated with its old epoch; operation UUIDs share one namespace.
    Cancellation changes no text, execution authority or native control state.
    """
    state = _copy(state); request_uuid(request_id); request_uuid(message_request_id)
    identifier(target_task_id)
    previous = next((m['cancellation'] for m in state['messages']
        if m['cancellation'] and m['cancellation']['request_id'] == request_id), None)
    if previous is not None:
        if ((previous['message_request_id'], previous['target_task_id']) != (message_request_id, target_task_id)
                or (expected_generation is not None and expected_generation != previous['generation'])
                or (expected_turn_id is not None and expected_turn_id != previous['turn_id'])
                or (expected_epoch is not None and expected_epoch != previous['ledger_epoch'])):
            raise ConversationError('request_conflict')
        return state, deepcopy(previous)
    if _item(state, request_id) is not None or any(m['promotion'] and m['promotion']['request_id'] == request_id for m in state['messages']):
        raise ConversationError('request_conflict')
    if type(expected_epoch) is not int or expected_epoch != state['ledger_epoch']:
        raise ConversationError('request_expired')
    if (target_task_id != state['head_task_id']
            or (expected_generation is not None and expected_generation != state['generation'])
            or (expected_turn_id is not None and expected_turn_id != state['turn_id'])):
        raise ConversationError('target_stale')
    item = _item(state, message_request_id)
    if item is None:
        raise ConversationError('request_missing')
    if item['status'] != 'queued':
        raise ConversationError('already_claimed')
    if item['interrupt_intent'] is not None:
        raise ConversationError('interrupt_already_issued')
    control = dict(request_id=request_id, message_request_id=message_request_id,
        target_task_id=target_task_id, generation=state['generation'], turn_id=state['turn_id'],
        ledger_epoch=state['ledger_epoch'])
    item['cancellation'] = control
    item['status'] = 'cancelled'
    return _finish(state), deepcopy(control)


def record_progress(state, *, generation, logical_turn, phase, native_terminal=None,
                    pending_tools=0,pending_approvals=0,unsettled_effects=0):
    """Trusted native observation only; never changes issuance/boundary authority."""
    state=_copy(state)
    if (generation,logical_turn)!=(state['generation'],state['turn_id']):
        raise ConversationError('turn_mismatch')
    if state['runtime_fault'] is not None: phase='unknown'
    elif state['closed']: phase='closed'
    state['native_progress']=dict(generation=generation,logical_turn=logical_turn,phase=phase,
        native_terminal=native_terminal,pending_tools=pending_tools,pending_approvals=pending_approvals,
        unsettled_effects=unsettled_effects)
    return validate(state)
