"""Bounded private launch-profile evidence for detached conversation admission.

A file is not permission authority: acceptance additionally requires maintained
source qualification, fresh runtime identity, and positive exact process/owner
identity. Unsupported/missing/uncertain evidence refuses without native effects.
"""
from __future__ import annotations

import base64
import json
import os
from dataclasses import asdict, fields
from pathlib import Path
import stat
import uuid

from . import identity
from .backends.base import Invocation
from .backends import get
from .backends import conversation_qualification as qualification
from .state_ownership import assert_owned
from .store import validate_task_id

MAX_PROFILE_BYTES = 256 * 1024
# Native ledger identifiers admit at most 256 characters. A control character
# occupies six JSON bytes, reserving the worst escaped representation exactly.
MAX_SESSION_CHARACTERS = 256


def _encode(document):
    return json.dumps(document, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode()


def validate_capacity(registry, task, profile, original_invocation, receipt):
    """Exact payload with actual owned binding and worst-case future session ID."""
    from .backends.conversation import ConversationRefused
    binding = _binding(task, registry)
    binding['session_id'] = '\x00' * MAX_SESSION_CHARACTERS
    document = dict(v=1, binding=binding, descriptor=task.conversation, profile=profile,
                    original=_invocation(original_invocation), qualification=asdict(receipt))
    if len(_encode(document)) > MAX_PROFILE_BYTES:
        raise ConversationRefused('private conversation profile capacity exceeds supported bound')


def _invocation(invocation):
    result = {field.name: getattr(invocation, field.name) for field in fields(invocation)}
    result['initial_input'] = base64.b64encode(invocation.initial_input).decode() if invocation.initial_input is not None else None
    return result


def _restore(value):
    value = dict(value)
    if value.get('initial_input') is not None:
        value['initial_input'] = base64.b64decode(value['initial_input'], validate=True)
    for name in ('extra_writable_paths', 'shared_scratch_directories', 'identity_markers', 'allowed_commands'):
        if value.get(name) is not None:
            value[name] = tuple(value[name])
    return Invocation(**value)


def _binding(task, registry):
    return dict(state_root=str(registry.state_layout.root), task_id=task.task_id, backend=task.backend, repo_path=str(task.repo_path),
        freedom=task.freedom, network=task.network, session_id=task.session_id,
        pid=task.proc.pid, start_time=task.start_time, markers=list(task.markers), owner=registry._owner)


def publish(registry, task, attachment):
    assert_owned(registry.state_layout)
    task_id = validate_task_id(task.task_id)
    if task.start_time is None or task.session_id is None:
        return  # No positive identity can be advertised yet.
    document = dict(v=1, binding=_binding(task, registry), descriptor=task.conversation,
        profile=task.conversation_context, original=_invocation(attachment.original_invocation),
        qualification=asdict(attachment.qualification_receipt))
    if type(task.session_id) is not str or len(task.session_id) > MAX_SESSION_CHARACTERS:
        raise ValueError('private conversation native session identifier exceeds its bound')
    raw = _encode(document)
    if len(raw) > MAX_PROFILE_BYTES:
        raise ValueError('private conversation profile exceeds its bound')
    directory = registry.state_layout.root / 'conversation-profiles'
    directory.mkdir(mode=0o700, exist_ok=True)
    directory_info = directory.lstat()
    if (not stat.S_ISDIR(directory_info.st_mode) or directory_info.st_uid != os.getuid()
            or stat.S_IMODE(directory_info.st_mode) & 0o077):
        raise ValueError('private conversation profile directory is not a directory')
    directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    pending = '.' + task_id + '.' + str(uuid.uuid4()) + '.pending'
    try:
        descriptor = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory_fd)
        try:
            with os.fdopen(descriptor, 'wb', closefd=False) as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(pending, task_id + '.json', src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        os.fsync(directory_fd)
    finally:
        try:
            os.unlink(pending, dir_fd=directory_fd)
        except FileNotFoundError:
            pass
        os.close(directory_fd)


def qualify(registry, source):
    if not qualification.ACCEPTED_PROFILES or not source.conversation or source.start_time is None or source.session_id is None:
        return False
    try:
        path = registry.state_layout.root / 'conversation-profiles' / (validate_task_id(source.task_id) + '.json')
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            directory_info = os.fstat(directory)
            if directory_info.st_uid != os.getuid() or stat.S_IMODE(directory_info.st_mode) & 0o077:
                return False
            descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=directory)
        finally:
            os.close(directory)
        try:
            info = os.fstat(descriptor)
            if (not stat.S_ISREG(info.st_mode) or info.st_size > MAX_PROFILE_BYTES
                    or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077):
                return False
            raw = os.read(descriptor, MAX_PROFILE_BYTES + 1)
        finally:
            os.close(descriptor)
        if len(raw) > MAX_PROFILE_BYTES:
            return False
        def pairs(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise ValueError('duplicate private profile field')
                result[key] = value
            return result
        document = json.loads(raw, object_pairs_hook=pairs)
        if document.get('v') != 1 or document.get('descriptor') != source.conversation:
            return False
        expected = dict(state_root=str(registry.state_layout.root), task_id=source.task_id, backend=source.backend, repo_path=source.repo_path,
            freedom=source.freedom, network=source.network, session_id=source.session_id,
            pid=source.pid, start_time=source.start_time, markers=source.markers, owner=source.owner)
        if document.get('binding') != expected:
            return False
        profile = document['profile']
        if qualification.fingerprint(profile) != source.conversation['profile_fingerprint']:
            return False
        original = _restore(document['original'])
        backend = get(source.backend)
        current, runtime = qualification.identify_runtime(backend)
        if current.version != profile['version']:
            return False
        from .conversation_managed_paths import from_record
        managed = from_record(registry.state_layout, source, original)
        canonical = qualification.canonical_profile(backend, original, profile, repo_path=Path(source.repo_path),
                                                      managed_paths=managed)
        scope = qualification.fingerprint(canonical)
        receipt = document['qualification']
        if (receipt['runtime_fingerprint'], receipt['profile_fingerprint'], receipt['launch_fingerprint']) != (
                runtime, scope, qualification.fingerprint(profile)):
            return False
        if not any(row.get('runtime_fingerprint') == runtime and row.get('profile_fingerprint') == scope
                   and row.get('evidence_sha256') == receipt['evidence_sha256'] for row in qualification.ACCEPTED_PROFILES):
            return False
        if identity.check_detail(identity.task_identity(source.pid, source.start_time, source.markers))[0] != 'alive':
            return False
        if source.owner is None or identity.identity_check(source.owner) != 'alive':
            return False
        return True
    except (OSError, ValueError, TypeError, KeyError, AttributeError, RecursionError):
        return False
