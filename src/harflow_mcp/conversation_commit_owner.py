"""Sealed native owner for one already issued, capped steering response.

The existing steering issuance is the only durable authority and queue. This
owner neither creates a conversation document nor accepts another caller input.
Physical exit/drain and the existing role classifier still decide commitment.
"""
from __future__ import annotations
import asyncio
import hashlib
from .conversation_documents import ConversationError
from .conversation_transport import ConversationInput
from .state_access import storage_operation
from .backends.base import Accumulator


def _issued(storage,run_id,task):
    run=storage.get_run(run_id,refresh_atomic=True)
    if run.get('parent_link'): raise ConversationError('nested_conversation_unsupported')
    turn=next((t for t in run.get('steering',{}).get('turns',[]) if t['task_id']==task.task_id),None)
    if turn is None: return None
    binding=next((b for b in run['steering'].get('conversation_delivery',{}).values()
                  if b.get('steering_episode_id')==turn['episode_id']),None)
    episode=next((ep for ep in run['steering']['episodes'] if ep['episode_id']==turn['episode_id']),None)
    proof=turn.get('conversation_commit_input')
    previous=turn.get('previous_session')
    if (binding is None or episode is None or not episode['hold'] or episode.get('dismiss_requested')
            or turn['state'] not in {'spawn_intent','running'} or turn.get('allow_fresh_context') is not False
            or previous is None or proof is None or run['status'] in {'cancelled','cancelling','completed','failed'}):
        raise ConversationError('commit_issuance_mismatch')
    from .steering_schema import captured_failure
    from .workflow_steering import target as current_target
    if (run['interaction_epoch']!=turn['commit_target']['interaction_epoch']
            or episode['steering_revision']!=turn['steering_revision']
            or current_target(run,storage,turn['commit_target'])!=turn['commit_target']):
        raise ConversationError('commit_authority_stale')
    state=binding['state']
    allowed_sources={state['head_task_id']} | {prior['task_id'] for prior in run['steering']['turns']
        if prior['episode_id']==turn['episode_id'] and prior['state']=='settled'
        and captured_failure(prior.get('observation'),prior['task_id']) is None
        and prior['observation']['session_id']==state['native_context_id']}
    if (not state['closed'] or state['runtime_fault'] is not None
            or previous['task_id'] not in allowed_sources or previous['session_id']!=state['native_context_id']
            or task.parent_task_id!=previous['task_id'] or task.session_id!=previous['session_id']
            or proof['source_task_id']!=previous['task_id'] or proof['source_session_id']!=previous['session_id']
            or type(task.max_turns) is not int or task.max_turns!=proof['cap'] or not 1<=task.max_turns<=turn['harness_turn_cap']
            or task.backend!=turn['candidate']['backend']):
        raise ConversationError('commit_issuance_mismatch')
    from . import store as task_store
    source=task_store.read(storage.state_layout.tasks_directory,previous['task_id'])
    if (source is None or source.status!='completed' or type(source.exit_code) is not int or source.exit_code!=0
            or not source.output_drained or source.session_id!=previous['session_id']
            or str(task.repo_path)!=source.repo_path
            or any(getattr(task,key,None)!=getattr(source,key,None) for key in ('backend','model','reasoning_effort','freedom','network','caller_approvals','caller_write_approvals'))
            or list(getattr(task,'allowed_commands',()) or ())!=list(source.allowed_commands or ())
            or source.max_turns is not None and task.max_turns>source.max_turns):
        raise ConversationError('commit_source_mismatch')
    raw=task.prompt.encode('utf-8')
    if len(raw)!=proof['bytes'] or hashlib.sha256(raw).hexdigest()!=proof['sha256']:
        raise ConversationError('commit_input_mismatch')
    return run,turn,proof


class SingleResponseOwner:
    def __init__(self,registry,task,port,storage,run_id):
        self.state_layout=registry.state_layout
        self.task,self.port,self.storage,self.run_id=task,port,storage,run_id
        self.initial_text=task.prompt
        self._issued=False;self._terminal=None;self._closed=False;self._unknown=False
        self._acc=Accumulator();self._text=''
        qualified=_issued(storage,run_id,task)
        if qualified is None: raise ConversationError('commit_issuance_missing')
        proof=qualified[2]
        profile=getattr(port,'profile',{})
        actual=profile.get('profile_name') or 'native'
        if actual!=proof['profile_name'] or port.session_id!=proof['source_session_id']:
            raise ConversationError('commit_profile_mismatch')

    @storage_operation
    async def before_issue(self,candidate,previous):
        if self._issued or previous is not None or candidate!=ConversationInput(self.task.task_id,self.initial_text):
            raise ConversationError('commit_input_mismatch')
        qualified=await asyncio.to_thread(_issued,self.storage,self.run_id,self.task)
        if qualified is None: raise ConversationError('commit_issuance_missing')
        self._issued=True

    async def next_input(self,previous): return None
    async def poll_control(self,identity): return None

    async def before_close(self,previous):
        if self._unknown: raise ConversationError('commit_delivery_unknown')
        if not self.port.can_issue: raise ConversationError('turn_active')
        if self._terminal is None or previous!=self.port.identity or self._terminal!=previous:
            raise ConversationError('commit_terminal_mismatch')
        self._closed=True
        return None

    async def observe(self,observation):
        if observation.kind=='session_ready':
            if observation.payload.get('session_id')!=self.task.session_id:
                raise ConversationError('commit_context_mismatch')
        if observation.kind=='input_issued' and (observation.identity is None or observation.identity.logical_turn!=self.task.task_id):
            raise ConversationError('commit_input_mismatch')
        if observation.kind in {'unknown','closed'} and (observation.kind!='closed' or not self._closed):
            self._unknown=True
        events=self.port.normalize_observation(observation,self._acc)
        for event in events:
            if event.get('kind')=='assistant_delta':
                self._text+=event.get('text','')
                if len(self._text.encode('utf-8'))>64*1024:
                    self._unknown=True
                    raise ConversationError('output_too_large')
        if observation.kind=='response_terminal':
            if observation.identity!=self.port.identity or observation.identity.logical_turn!=self.task.task_id:
                raise ConversationError('commit_terminal_mismatch')
            self._terminal=observation.identity
            # Preserve the complete bounded frame for the existing physical
            # result classifier, rather than the monitor's display tail.
            self.task.acc.summary=self._acc.summary or self._text


def for_task(registry,task,port,*,profile_fingerprint):
    from .workflow_hooks import owner
    from .workflows import WorkflowStore
    association=owner(registry.log_dir,task.task_id,strict=True)
    if association is None:return None
    storage=WorkflowStore(root=registry.state_layout.root)
    run=storage.get_run(association['workflow_run_id'],refresh_atomic=True)
    turn=next((t for t in run.get('steering',{}).get('turns',[]) if t['task_id']==task.task_id),None)
    if turn is None:return None
    # An issued steering task may never fall through to an ordinary queue owner.
    return SingleResponseOwner(registry,task,port,storage,association['workflow_run_id'])
