"""Internal task lifecycle adapter for independently qualified conversation launches.

No public run option enables this module. The launch owner supplies a trusted
factory in its own process; serialized context alone is refused. Detached owners
must install the same source qualifier independently before revalidation.
"""
from __future__ import annotations

import copy
import inspect
from dataclasses import dataclass, replace
from typing import Any, Callable
from pathlib import Path

from .backends.base import Accumulator
from .backends.conversation import ConversationLaunch, ConversationRefused
from .conversation_transport import ConversationDriver, ConversationInput
from .state_ownership import StateOwnershipError


@dataclass(frozen=True)
class ConversationAttachment:
    launch: ConversationLaunch
    owner_factory: Callable[[Any], Any]
    idle_timeout: float = 1.0
    operation_timeout: float = 120.0
    original_invocation: Any = None
    qualification_receipt: Any = None


async def preflight(registry, backend, invocation, policy, *, factory=None):
    """Source identity/version/hash work before any shared scratch leaf lock."""
    if invocation.conversation_context is not None:
        raise ConversationRefused("serialized conversation context is not launch authority")
    factory = factory or registry._conversation_launch_factory
    if factory is None:
        return None
    backend.assert_safe(invocation, policy['freedom'], policy.get('network'))
    inspect_source = getattr(factory, 'preflight', None)
    result = inspect_source(backend, invocation, policy) if callable(inspect_source) else factory(backend, invocation, policy)
    if inspect.isawaitable(result):
        result = await result
    return result


def prepare(registry, backend, invocation, policy, preflight_result, *, factory=None):
    """Pure final mapping/receipt checks; no subprocess, hashing or awaited I/O.

    A production factory exposes preflight + finalize. A test-only simple factory
    may return an attachment directly from preflight, provided later allocation
    changed no execution authority. Async finalizers explicitly refuse.
    """
    factory = factory or registry._conversation_launch_factory
    if factory is None or preflight_result is None:
        return None
    original = copy.deepcopy(invocation)
    finalize = getattr(factory, 'finalize', None)
    attachment = finalize(preflight_result, backend, invocation, policy) if callable(finalize) else preflight_result
    if inspect.isawaitable(attachment):
        if inspect.iscoroutine(attachment):
            attachment.close()
        raise ConversationRefused("conversation finalizer must not await under shared scratch ownership")
    if attachment is None:
        return None
    if not isinstance(attachment, ConversationAttachment):
        raise ConversationRefused("conversation factory returned no qualified attachment")
    converted = attachment.launch.invocation
    if (converted.conversation_context is None or
            replace(converted, argv=original.argv, env=original.env,
                    identity_markers=original.identity_markers, conversation_context=None) != original):
        raise ConversationRefused("conversation conversion changed original grant/cap/input provenance")
    context = converted.conversation_context
    if context.get('argv') != converted.argv or context.get('env') != dict(converted.env):
        raise ConversationRefused("conversation launch differs from qualified profile")
    if context.get('initial_prompt') != policy['prompt']:
        raise ConversationRefused("conversation launch changed initial caller input")
    if context.get('backend') != backend.name or context.get('freedom') != policy['freedom']:
        raise ConversationRefused("conversation profile authority mismatch")
    if context.get('caller_approvals') != original.caller_approvals:
        raise ConversationRefused("conversation transport changed caller approval opt-in")
    if attachment.launch.port.profile != context:
        raise ConversationRefused("conversation native port differs from launch profile")
    return attachment


class _TaskOwner:
    def __init__(self, registry, task, owner):
        self.registry, self.task, self.owner = registry, task, owner

    async def before_issue(self, candidate, previous):
        await self.owner.before_issue(candidate, previous)

    async def poll_control(self, identity):
        return await self.owner.poll_control(identity)

    async def next_input(self, previous):
        return await self.owner.next_input(previous)

    async def before_close(self, previous):
        return await self.owner.before_close(previous)

    async def observe(self, observation):
        # Durable owner evidence precedes best-effort product presentation.
        await self.owner.observe(observation)
        task = self.task
        if observation.kind == 'input_issued':
            task.conversation_responses_issued += 1
            previous = task.acc
            task.acc = Accumulator(session_id=task.session_id,
                observed_model=copy.deepcopy(previous.observed_model),
                available_tool_count=previous.available_tool_count,
                mcp_servers=copy.deepcopy(previous.mcp_servers))
            if not observation.payload.get('initial'):
                from . import tasks
                tasks._write_event(task, 'user_message', {
                    'text': observation.payload['text'],
                    'source': 'conversation_delivery',
                    'conversation_generation': observation.identity.generation,
                    'conversation_turn_id': observation.identity.logical_turn,
                    'semantic_consumption': 'unknown',
                })
        elif observation.kind == 'session_ready':
            sid = observation.payload.get('session_id')
            if sid:
                if task.session_id and task.session_id != sid:
                    raise ConversationRefused('native conversation session changed')
                task.session_id = task.acc.session_id = sid
                publish_source_profile(self.registry, task)
                self.registry.persist(task)
        elif observation.kind == 'unknown':
            task.acc.is_error = True
            task.bridge_notices.append(observation.payload.get('reason', 'Native conversation outcome unknown'))
        if observation.kind not in {'native_event', 'response_terminal'}:
            return
        if observation.kind == 'response_terminal':
            task.conversation_terminal = observation.payload
            task.acc.terminal = observation.payload['native']
            task.acc.turn_open = False
            task.acc.is_error = observation.payload['status'] not in {'completed', 'end_turn'}
        from . import tasks
        try:
            normalized = task.conversation_driver.port.normalize_observation(observation, task.acc)
            for event in normalized:
                fields = dict(event)
                kind = fields.pop('kind')
                if observation.identity is not None:
                    fields.update(conversation_generation=observation.identity.generation,
                                  conversation_turn_id=observation.identity.logical_turn)
                if kind == 'assistant_delta':
                    text = fields.get('text')
                    if isinstance(text, str):
                        task.acc.summary = ((task.acc.summary or '') + text)[-16384:]
                        task.acc.saw_final_message = True
                tasks._write_event(task, kind, fields)
        except StateOwnershipError:
            raise
        except Exception:
            task.acc.normalize_errors += 1
        if observation.kind == 'response_terminal':
            task.acc.result_count = max(task.acc.result_count, 1)
        self.registry.persist(task)


def bind(registry, task, attachment):
    if task.proc is None or task.proc.stdin is None:
        raise ConversationRefused('conversation launch has no native stdin pipe')
    owner = _TaskOwner(registry, task, attachment.owner_factory(task))
    if task.conversation is not None and attachment.qualification_receipt is not None:
        from .conversation_profile_store import validate_capacity
        validate_capacity(registry, task, attachment.launch.port.profile,
                          attachment.original_invocation, attachment.qualification_receipt)
    driver = ConversationDriver(attachment.launch.port, task.proc.stdin, owner,
        initial_input=ConversationInput(task.task_id, task.prompt),
        idle_timeout=attachment.idle_timeout, operation_timeout=attachment.operation_timeout)
    task.conversation_driver = driver
    task.conversation_context = copy.deepcopy(attachment.launch.invocation.conversation_context)
    task._conversation_attachment = attachment
    return driver


def qualify_saved_source(registry, source) -> bool:
    """Verify live in-process origin without treating a descriptor as authority.

    Detached callers currently refuse until accepted qualification receipts and
    source-bound private profile evidence can be independently revalidated. A
    descriptor, old feature receipt or remembered version never enables control.
    """
    import os
    from .backends.conversation_qualification import fingerprint
    live = registry._tasks.get(source.task_id)
    if live is None or live.conversation_driver is None or live.conversation_context is None:
        from .conversation_profile_store import qualify
        return qualify(registry, source)
    descriptor = getattr(source, 'conversation', None)
    if descriptor is None or descriptor != live.conversation:
        return False
    if descriptor.get('profile_fingerprint') != fingerprint(live.conversation_context):
        return False
    if (live._process_owner_pid != os.getpid() or live.proc is None or live.proc.returncode is not None
            or source.pid != live.proc.pid or source.session_id != live.session_id
            or source.start_time != live.start_time or source.markers != list(live.markers)
            or source.owner != registry._owner
            or str(source.repo_path) != str(live.repo_path) or source.backend != live.backend):
        return False
    return live.conversation_driver.port.unknown is None and not live.conversation_driver.closed


class QualifiedConversationFactory:
    """Production explicit-selection factory, independently installed in each owner."""
    def __init__(self, registry, *, profile_name: str = 'native'):
        if profile_name not in {'native', 'shipped-medium35-high'}:
            raise ConversationRefused('unknown native conversation profile')
        self.registry, self.profile_name = registry, profile_name

    async def preflight(self, backend, invocation, policy):
        import asyncio
        from .backends.conversation_qualification import runtime_preflight
        return await asyncio.to_thread(runtime_preflight, backend, invocation, policy)

    def finalize(self, preflight_result, backend, invocation, policy):
        from .backends.conversation_qualification import prepare_from_preflight
        launch, receipt = prepare_from_preflight(preflight_result, backend, invocation,
            repo_path=policy['repo_path'], freedom=policy['freedom'], network=policy.get('network'),
            profile_name=None if self.profile_name == 'native' else self.profile_name,
            managed_paths=policy.get('managed_paths'))
        def owner_factory(task):
            from .conversation_live_owner import for_task
            return for_task(self.registry, task, launch.port, profile_fingerprint=receipt.launch_fingerprint)
        return ConversationAttachment(launch, owner_factory, original_invocation=copy.deepcopy(invocation),
                                      qualification_receipt=receipt)


class _ProbeConversationFactory(QualifiedConversationFactory):
    """Explicit maintainer probe only; never selected by run/ctl parameters."""
    async def preflight(self, backend, invocation, policy):
        import asyncio
        from .backends.conversation_qualification import _runtime_preflight
        return await asyncio.to_thread(_runtime_preflight, backend, invocation, policy)

    def finalize(self, preflight_result, backend, invocation, policy):
        from .backends.conversation_native import conversation_profile, restore_conversation_port
        from .backends.conversation_qualification import canonical_profile, fingerprint, verify_runtime_preflight
        verify_runtime_preflight(preflight_result, backend)
        profile = conversation_profile(backend, invocation, version=preflight_result.identity.version,
            freedom=policy['freedom'], network=policy.get('network'),
            profile_name=None if self.profile_name == 'native' else self.profile_name,
            configuration_witness=preflight_result.configuration_witness)
        profile.setdefault('cwd', str(policy['repo_path']))
        profile['requested_network'] = policy.get('network')
        scope = fingerprint(canonical_profile(backend, invocation, profile, repo_path=policy['repo_path'],
                                               managed_paths=policy.get('managed_paths')))
        self.candidate_evidence = dict(runtime_fingerprint=preflight_result.runtime_fingerprint,
            profile_fingerprint=scope, launch_fingerprint=fingerprint(profile),
            backend=backend.name, version=preflight_result.identity.version)
        converted = replace(invocation, argv=profile['argv'], env=profile['env'], conversation_context=profile,
                            identity_markers=tuple(profile['argv'][:2]))
        launch = ConversationLaunch(converted, restore_conversation_port(profile, validated_profile=profile))
        def owner_factory(task):
            from .conversation_live_owner import for_task
            return for_task(self.registry, task, launch.port, profile_fingerprint=fingerprint(profile))
        return ConversationAttachment(launch, owner_factory)


def _probe_factory(registry, *, profile_name='native'):
    """Private hook for explicitly authorized live qualification, never automatic."""
    return _ProbeConversationFactory(registry, profile_name=profile_name)


def publish_source_profile(registry, task):
    """Publish independently qualified private correlation evidence, never grants."""
    if getattr(task, 'conversation', None) is None:
        return  # Sealed one-response commit tasks expose no Queue/Now admission.
    attachment = getattr(task, '_conversation_attachment', None)
    if attachment is None or attachment.qualification_receipt is None:
        return
    from .conversation_profile_store import publish
    publish(registry, task, attachment)


async def reap_failed_launch(task):
    """Owned pipe/exit cleanup after lost state authority; never publishes done."""
    import asyncio
    if task.proc is None:
        return
    async def drain(stream):
        if stream is not None:
            while await stream.read(65536):
                pass
    jobs = [asyncio.create_task(drain(stream)) for stream in (task.proc.stdout, task.proc.stderr)]
    jobs.append(asyncio.create_task(task.proc.wait()))
    try:
        await asyncio.wait_for(asyncio.gather(*jobs), timeout=10)
    except (asyncio.TimeoutError, OSError):
        task.bridge_notices.append('Failed conversation launch cleanup has unobserved pipe/process settlement.')
    finally:
        for job in jobs:
            if not job.done():
                job.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)
