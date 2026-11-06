"""Strict bounded internal documents for conversation_delivery_v1."""
from __future__ import annotations
import json
import uuid

CONTRACT = 'conversation_delivery_v1'
MAX_MESSAGE_BYTES = 32 * 1024
MAX_TEXT_BYTES = 1024 * 1024
MAX_OUTSTANDING = 32
MAX_HISTORY = 64
MAX_DOCUMENT_BYTES = 4 * 1024 * 1024
COMPLETION_RESERVE_BYTES = 1024 * 1024
STATES = {'queued', 'claimed', 'consumed', 'unknown', 'cancelled'}

class ConversationError(ValueError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code

def utf8_size(value):
    try:
        return len(value.encode('utf-8'))
    except (AttributeError, UnicodeError):
        raise ConversationError('invalid_utf8') from None

def identifier(value):
    if not isinstance(value, str) or not value or utf8_size(value) > 256 or any(ord(c) < 32 for c in value):
        raise ConversationError('invalid_identity')
    return value

def request_uuid(value):
    if not isinstance(value, str):
        raise ConversationError('invalid_request_id')
    try:
        if str(uuid.UUID(value)) != value:
            raise ValueError
    except ValueError:
        raise ConversationError('invalid_request_id') from None
    return value

def encoded(document):
    try:
        return json.dumps(document, ensure_ascii=False, allow_nan=False, separators=(',', ':'), sort_keys=True).encode('utf-8')
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ConversationError('invalid_document') from None

def validate(document):
    if not isinstance(document, dict) or set(document) != {'contract','conversation_id','head_task_id','native_context_id','generation','turn_id','boundary','closed','sequence','messages','ledger_epoch','cumulative_issuances','accounting','runtime_fault','native_progress'}:
        raise ConversationError('invalid_document')
    if document['contract'] != CONTRACT or type(document['generation']) is not int or document['generation'] < 0 or type(document['sequence']) is not int or document['sequence'] < 0:
        raise ConversationError('invalid_document')
    progress=document['native_progress']
    if not isinstance(progress,dict) or set(progress)!={'generation','logical_turn','phase','native_terminal','pending_tools','pending_approvals','unsettled_effects'}:
        raise ConversationError('invalid_document')
    if type(progress['generation']) is not int or progress['generation']!=document['generation'] or progress['logical_turn']!=document['turn_id']:
        raise ConversationError('invalid_document')
    if progress['phase'] not in {'starting','issuing','responding','interrupting','waiting_tools','boundary','unknown','closed'}:
        raise ConversationError('invalid_document')
    if progress['native_terminal'] is not None: identifier(progress['native_terminal'])
    for key in ('pending_tools','pending_approvals','unsettled_effects'):
        if type(progress[key]) is not int or not 0<=progress[key]<=4096: raise ConversationError('invalid_document')
    if document['closed'] and progress['phase']!='closed': raise ConversationError('invalid_document')
    if progress['phase']=='closed' and not document['closed']: raise ConversationError('invalid_document')
    fault=document['runtime_fault']
    if fault is not None and progress['phase']!='unknown': raise ConversationError('invalid_document')
    if fault is not None:
        if not isinstance(fault,dict) or set(fault)!={'code','generation','turn_id'}: raise ConversationError('invalid_document')
        identifier(fault['code']); identifier(fault['turn_id'])
        if type(fault['generation']) is not int or fault['generation']!=document['generation'] or fault['turn_id']!=document['turn_id']:
            raise ConversationError('invalid_document')
    import math
    accounting=document['accounting']
    if not isinstance(accounting,dict) or set(accounting)!={'settled_responses','accounted_generation','observed_cost_usd','usage_complete','cost_basis','last_cost_usd','last_cost_basis'}:
        raise ConversationError('invalid_document')
    if type(accounting['settled_responses']) is not int or accounting['settled_responses']<0 or type(accounting['accounted_generation']) is not int or accounting['accounted_generation'] < -1 or type(accounting['usage_complete']) is not bool:
        raise ConversationError('invalid_document')
    for key in ('cost_basis','last_cost_basis'):
        if accounting[key] not in {'response_local','native_cumulative','unavailable'}: raise ConversationError('invalid_document')
    for key in ('observed_cost_usd','last_cost_usd'):
        cost=accounting[key]
        if cost is not None and (type(cost) not in (int,float) or not math.isfinite(cost) or cost<0): raise ConversationError('invalid_document')
    if accounting['accounted_generation']>document['generation'] or (accounting['usage_complete'] and (accounting['observed_cost_usd'] is None or accounting['cost_basis']=='unavailable' or accounting['settled_responses']==0)):
        raise ConversationError('invalid_document')
    for counter in ('ledger_epoch','cumulative_issuances'):
        if type(document[counter]) is not int or document[counter] < 0: raise ConversationError('invalid_document')
    request_uuid(document['conversation_id'])
    for key in ('conversation_id','head_task_id','turn_id'):
        identifier(document[key])
    if document['native_context_id'] is not None:
        identifier(document['native_context_id'])
    if type(document['boundary']) is not bool or type(document['closed']) is not bool or not isinstance(document['messages'], list):
        raise ConversationError('invalid_document')
    seen = set()
    sequence = 0
    for item in document['messages']:
        if not isinstance(item, dict) or set(item) != {'request_id','text','delivery','target_task_id','original_target_task_id','source_generation','source_turn_id','source_epoch','sequence','status','claim','receipt','promotion','interrupt_intent','cancellation'}:
            raise ConversationError('invalid_document')
        request_uuid(item['request_id'])
        if item['request_id'] in seen or type(item['sequence']) is not int or item['sequence'] <= sequence or item['sequence'] > document['sequence']:
            raise ConversationError('invalid_document')
        seen.add(item['request_id']); sequence = item['sequence']
        if not isinstance(item['text'],str) or not item['text'] or utf8_size(item['text']) > MAX_MESSAGE_BYTES or item['delivery'] not in ('queue','now') or item['status'] not in STATES:
            raise ConversationError('invalid_document')
        identifier(item['target_task_id']); identifier(item['original_target_task_id']); identifier(item['source_turn_id'])
        if type(item['source_generation']) is not int or item['source_generation'] < 0:
            raise ConversationError('invalid_document')
        if type(item['source_epoch']) is not int or not 0 <= item['source_epoch'] <= document['ledger_epoch']:
            raise ConversationError('invalid_document')
        intent=item['interrupt_intent']
        if intent is not None:
            if not isinstance(intent,dict) or set(intent)!={'session_id','turn_id','generation','logical_turn'}:
                raise ConversationError('invalid_document')
            for key in ('session_id','turn_id','logical_turn'): identifier(intent[key])
            if item['delivery']!='now' and item['promotion'] is None: raise ConversationError('invalid_document')
            if type(intent['generation']) is not int or intent['generation']<0 or intent['session_id']!=document['native_context_id']:
                raise ConversationError('invalid_document')
        promotion = item['promotion']
        if promotion is not None:
            if not isinstance(promotion,dict) or set(promotion) != {'request_id','message_request_id','target_task_id','generation','turn_id','ledger_epoch'}:
                raise ConversationError('invalid_document')
            request_uuid(promotion['request_id']); identifier(promotion['target_task_id']); identifier(promotion['turn_id'])
            if promotion['message_request_id'] != item['request_id'] or type(promotion['generation']) is not int or promotion['generation'] < 0 or type(promotion['ledger_epoch']) is not int or not 0 <= promotion['ledger_epoch'] <= document['ledger_epoch']:
                raise ConversationError('invalid_document')
        cancellation = item['cancellation']
        if cancellation is not None:
            if not isinstance(cancellation, dict) or set(cancellation) != {'request_id','message_request_id','target_task_id','generation','turn_id','ledger_epoch'}:
                raise ConversationError('invalid_document')
            request_uuid(cancellation['request_id']); identifier(cancellation['target_task_id']); identifier(cancellation['turn_id'])
            if (cancellation['message_request_id'] != item['request_id'] or item['status'] != 'cancelled'
                    or type(cancellation['generation']) is not int or cancellation['generation'] < 0
                    or type(cancellation['ledger_epoch']) is not int or not 0 <= cancellation['ledger_epoch'] <= document['ledger_epoch']):
                raise ConversationError('invalid_document')
        claim = item['claim']
        if claim is not None:
            if not isinstance(claim,dict) or set(claim) != {'claim_id','origin','conversation_id','native_context_id','source_task_id','source_generation','source_turn_id','request_id','destination_task_id','destination_turn_id','destination_generation'}:
                raise ConversationError('invalid_document')
            request_uuid(claim['claim_id'])
            if claim['origin'] not in {'queue_drain','now_delivery'}: raise ConversationError('invalid_document')
            for key in ('conversation_id','native_context_id','source_task_id','source_turn_id','destination_task_id','destination_turn_id'):
                identifier(claim[key])
            if (claim['request_id'] != item['request_id'] or type(claim['source_generation']) is not int
                    or claim['conversation_id'] != document['conversation_id']
                    or claim['native_context_id'] != document['native_context_id']
                    or type(claim['destination_generation']) is not int
                    or claim['destination_generation'] != claim['source_generation'] + 1
                    or claim['destination_turn_id'] == claim['source_turn_id']):
                raise ConversationError('invalid_document')
        if (item['status'] in ('claimed','consumed','unknown')) != (claim is not None):
            raise ConversationError('invalid_document')
        receipt = item['receipt']
        if receipt is not None:
            if not isinstance(receipt,dict) or set(receipt) != {'claim','task_id','turn_id','generation','kind'} or receipt['claim'] != claim or receipt['kind'] != 'input_consumed':
                raise ConversationError('invalid_document')
            identifier(receipt['task_id']); identifier(receipt['turn_id'])
            if (type(receipt['generation']) is not int or receipt['generation'] != claim['destination_generation']
                    or receipt['task_id'] != claim['destination_task_id'] or receipt['turn_id'] != claim['destination_turn_id']):
                raise ConversationError('invalid_document')
        if (item['status'] == 'consumed') != (receipt is not None):
            raise ConversationError('invalid_document')
    all_ids = [m['request_id'] for m in document['messages']] + [m['promotion']['request_id'] for m in document['messages'] if m['promotion']] + [m['cancellation']['request_id'] for m in document['messages'] if m['cancellation']]
    if len(all_ids) != len(set(all_ids)): raise ConversationError('invalid_document')
    for item in document['messages']:
        if item['status'] in ('claimed','unknown'):
            claim = item['claim']
            if (document['head_task_id'],document['turn_id'],document['generation']) != (claim['destination_task_id'],claim['destination_turn_id'],claim['destination_generation']):
                raise ConversationError('invalid_document')
    if sum(m['status'] in ('queued','claimed','unknown') for m in document['messages']) > MAX_OUTSTANDING:
        raise ConversationError('document_full')
    if sum(m['status'] in ('claimed','unknown') for m in document['messages']) > 1:
        raise ConversationError('invalid_document')
    if (sum(utf8_size(item['text']) for item in document['messages']) > MAX_TEXT_BYTES
            or len(document['messages']) > MAX_HISTORY + MAX_OUTSTANDING or len(encoded(document)) > MAX_DOCUMENT_BYTES):
        raise ConversationError('document_full')
    return document
