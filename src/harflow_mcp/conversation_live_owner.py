"""Durable standalone owner callbacks for a qualified native live conversation.

This owner never publishes physical task settlement. It joins correlated native
response evidence to one shared conversation ledger; process ownership remains
with the task monitor. Existing ledgers are never replayed by a replacement owner.
"""
from __future__ import annotations

import asyncio
import uuid

from . import conversation_delivery as delivery
from .conversation_documents import ConversationError
from .conversation_store import ConversationStore
from .conversation_transport import ConversationInput
from .state_access import storage_operation

_NAMESPACE = uuid.UUID('eeec811c-fbbd-4786-86c7-ce4fde111ade')


def conversation_id(task_id):
    return str(uuid.uuid5(_NAMESPACE, task_id))


class ConversationOwner:
    """Created only by the independently qualified launch's owner factory.

    Initial issue has a preexisting durable generation/turn reservation. A second
    owner finding it refuses, including after a crash before the first write.
    """
    def __init__(self, layout, task_id, initial_text, port):
        self.state_layout = layout
        self.task_id, self.initial_text, self.port = task_id, initial_text, port
        self.conversation_id = conversation_id(task_id)
        self.ledger = ConversationStore(layout, self.conversation_id)
        self._terminal = None
        self._create()

    @storage_operation
    def _create(self):
        def create(old):
            if old is not None:
                raise ConversationError('conversation_owner_exists')
            state = delivery.new_state(self.conversation_id, self.task_id,
                self.port.session_id, 1, self.task_id)
            return state, None
        self.ledger.update(create)

    def _matches(self, state, identity):
        return (identity is not None
            and identity.session_id == state['native_context_id']
            and identity.generation == state['generation']
            and identity.logical_turn == state['turn_id'])

    @storage_operation
    async def before_issue(self, candidate, previous):
        def verify(state):
            if state['closed'] or state['head_task_id'] != self.task_id:
                raise ConversationError('target_stale')
            if previous is None:
                if state['generation'] != 1 or state['turn_id'] != self.task_id or candidate != ConversationInput(self.task_id, self.initial_text):
                    raise ConversationError('initial_issue_mismatch')
            else:
                item = next((m for m in state['messages'] if m['status'] == 'claimed'
                    and m['claim']['destination_turn_id'] == candidate.logical_turn), None)
                if (item is None or item['text'] != candidate.text
                        or item['claim']['destination_task_id'] != self.task_id
                        or item['claim']['destination_generation'] != previous.generation + 1):
                    raise ConversationError('claim_missing')
            return state, None
        await asyncio.to_thread(self.ledger.update, verify)

    @storage_operation
    async def next_input(self, previous):
        if not self.port.can_issue:
            return None
        await self._settle_terminal()
        claim_id, logical_turn = str(uuid.uuid4()), str(uuid.uuid4())
        def claim(state):
            if not self._matches(state, previous):
                raise ConversationError('turn_mismatch')
            state, item = delivery.claim_next(state, claim_id=claim_id,
                destination_task_id=self.task_id, destination_turn_id=logical_turn)
            return state, ConversationInput(logical_turn, item['text']) if item else None
        return await asyncio.to_thread(self.ledger.update, claim)

    @storage_operation
    async def before_close(self, previous):
        if not self.port.can_issue:
            raise ConversationError('turn_active')
        await self._settle_terminal()
        claim_id, logical_turn = str(uuid.uuid4()), str(uuid.uuid4())
        def finish(state):
            if not self._matches(state, previous):
                raise ConversationError('turn_mismatch')
            state, item = delivery.claim_next(state, claim_id=claim_id,
                destination_task_id=self.task_id, destination_turn_id=logical_turn)
            if item:
                return state, ConversationInput(logical_turn, item['text'])
            return delivery.close(state), None
        return await asyncio.to_thread(self.ledger.update, finish)

    @storage_operation
    async def observe(self, observation):
        await self._publish_progress(observation)
        if observation.kind == 'session_ready':
            context = observation.payload.get('session_id')
            await asyncio.to_thread(self.ledger.update, lambda state: (
                delivery.bind_native_context(state, context_id=context, generation=state['generation']), None))
            return
        if observation.kind == 'interrupt_intent':
            try:
                await asyncio.to_thread(self.ledger.update, lambda state: (
                    delivery.record_interrupt_intent(state,
                        request_id=observation.payload.get('request_id'),
                        native_identity=observation.payload.get('expected_identity')), None))
            except ConversationError as exc:
                if exc.code not in ('interrupt_not_pending', 'request_missing'):
                    raise
                from .backends.conversation import ConversationRefused
                raise ConversationRefused('native interrupt intent was withdrawn before issue') from exc
            return
        if observation.kind in ('unknown', 'closed'):
            def uncertain(state):
                if observation.kind == 'closed' and state['closed']:
                    return state, None
                item = next((m for m in state['messages'] if m['status'] in ('claimed', 'unknown')), None)
                if item:
                    state = delivery.mark_unknown(state, claim_id=item['claim']['claim_id'])
                if state['runtime_fault'] is None:
                    state = delivery.mark_runtime_fault(state, code='native_runtime_unknown',
                        generation=state['generation'], turn_id=state['turn_id'])
                # No response proof, no boundary. Even zero-message faults pin state.
                return state, None
            await asyncio.to_thread(self.ledger.update, uncertain)
            return
        if observation.kind == 'response_terminal':
            self._terminal = observation
        if observation.kind not in ('response_terminal', 'tool_terminal', 'approval_expired'):
            return
        await self._settle_terminal()

    async def _publish_progress(self, observation):
        if observation.kind not in {'session_ready','input_issued','input_accepted','interrupt_intent',
                'interrupt_acknowledged','tool_pending','tool_terminal','approval_pending','approval_expired',
                'response_terminal','unknown','closed'}:
            return
        identity=observation.identity
        terminal=getattr(self.port,'terminal',None)
        tools=len(getattr(self.port,'pending_tools',()))
        approvals=len(getattr(self.port,'pending_approvals',()))
        effects=len(getattr(self.port,'unsettled_effects',()))
        if getattr(self.port,'unknown',False) or observation.kind=='unknown': phase='unknown'
        elif observation.kind=='closed': phase='closed'
        elif terminal is not None:
            phase='boundary' if self.port.can_issue else 'waiting_tools'
        elif observation.kind=='interrupt_intent' or getattr(self.port,'interrupt_requested',False): phase='interrupting'
        elif tools or approvals or effects: phase='waiting_tools'
        elif observation.kind=='input_issued': phase='issuing'
        elif identity is not None: phase='responding'
        else: phase='starting'
        key=(getattr(identity,'generation',None),getattr(identity,'logical_turn',None),phase,terminal,tools,approvals,effects)
        if key==getattr(self,'_last_progress',None): return
        def publish(state):
            generation=state['generation'] if identity is None else identity.generation
            logical=state['turn_id'] if identity is None else identity.logical_turn
            if (generation,logical)!=(state['generation'],state['turn_id']): return state,None
            observed_phase='unknown' if observation.kind=='closed' and not state['closed'] else phase
            return delivery.record_progress(state,generation=generation,logical_turn=logical,phase=observed_phase,
                native_terminal=terminal,pending_tools=tools,pending_approvals=approvals,unsettled_effects=effects),None
        await asyncio.to_thread(self.ledger.update,publish)
        self._last_progress=key

    async def _settle_terminal(self):
        terminal = self._terminal
        if terminal is None or not self.port.can_issue:
            return
        if terminal.identity != self.port.identity or terminal.identity.turn_id is None:
            raise ConversationError('native_terminal_mismatch')
        def finished(state):
            if not self._matches(state, terminal.identity):
                raise ConversationError('turn_mismatch')
            item = next((m for m in state['messages'] if m['status'] in ('claimed', 'unknown')), None)
            if item:
                claim = item['claim']
                state = delivery.record_receipt(state, receipt=dict(claim=claim,
                    task_id=self.task_id, turn_id=terminal.identity.logical_turn,
                    generation=terminal.identity.generation, kind='input_consumed'))
            state = delivery.record_response(state, generation=terminal.identity.generation, turn_id=terminal.identity.logical_turn)
            return delivery.observe_boundary(state, generation=terminal.identity.generation,
                turn_id=terminal.identity.logical_turn), None
        await asyncio.to_thread(self.ledger.update, finished)

    @storage_operation
    async def poll_control(self, identity):
        """Read durable cross-process Now intent; driver deduplicates request IDs.

        An old generation can never interrupt a later response. A native terminal
        winning the race leaves Now queued for the subsequent boundary claim.
        """
        if identity is None or self.port.terminal is not None or self.port.unknown:
            return None
        if self.port.pending_tools or self.port.pending_approvals or self.port.unsettled_effects:
            return None
        state = await asyncio.to_thread(self.ledger.read)
        if not self._matches(state, identity):
            return None
        selected = next((m for m in state['messages'] if m['status'] == 'queued' and (
            (m['delivery'] == 'now' and m['source_generation'] == identity.generation
                and m['source_turn_id'] == identity.logical_turn)
            or (m['promotion'] is not None and m['promotion']['generation'] == identity.generation
                and m['promotion']['turn_id'] == identity.logical_turn))), None)
        if selected is None:
            return None
        from .conversation_transport import ConversationInterrupt
        return ConversationInterrupt(identity, selected['request_id'])


@storage_operation
def admit(owner, text, *, request_id, mode, expected):
    """Shared leaf CAS used by ctl/MCP after their authority/profile checks."""
    def publish(state):
        if expected['conversation_id'] != owner.conversation_id or expected['head_task_id'] != owner.task_id:
            raise ConversationError('target_stale')
        return delivery.admit(state, request_id=request_id, text=text, delivery=mode,
            target_task_id=owner.task_id, original_target_task_id=owner.task_id,
            expected_generation=expected['generation'], expected_turn_id=expected['turn_id'],
            expected_epoch=expected['ledger_epoch'])
    return owner.ledger.update(publish)


def for_task(registry, task, port, *, profile_fingerprint):
    """Qualified attachment factory binds a descriptive receipt before persist.

    The caller has independently validated executable/configuration. This hash
    is correlation metadata, never sufficient to authorize another launch.
    """
    from .conversation_documents import CONTRACT
    if task.max_turns is not None and getattr(port, 'profile', {}).get('max_turns') != task.max_turns:
        raise ConversationError('conversation_cap_unsupported')
    if (type(profile_fingerprint) is not str or len(profile_fingerprint) != 64
            or any(c not in '0123456789abcdef' for c in profile_fingerprint)):
        raise ConversationError('profile_fingerprint_invalid')
    from .conversation_commit_owner import for_task as commit_owner
    sealed=commit_owner(registry,task,port,profile_fingerprint=profile_fingerprint)
    if sealed is not None:
        return sealed
    from .conversation_workflows import for_task as workflow_owner
    selected = workflow_owner(registry, task, port, profile_fingerprint=profile_fingerprint)
    if selected is not None:
        return selected
    owner = ConversationOwner(registry.state_layout, task.task_id, task.prompt, port)
    task.conversation = dict(contract=CONTRACT, conversation_id=owner.conversation_id,
        transport=port.transport, profile_fingerprint=profile_fingerprint)
    return owner


def ledger_binding(layout, task_id):
    """Read/write binding for a separately authorized ctl/MCP admission.

    This does not create an owner, qualify a source or permit a launch. It only
    locates the same durable queue so a detached owner can poll admitted intent.
    """
    from types import SimpleNamespace
    cid = conversation_id(task_id)
    return SimpleNamespace(state_layout=layout, task_id=task_id,
        conversation_id=cid, ledger=ConversationStore(layout, cid))
