"""Owner-local proof of managed scratch grant roles; never caller metadata.

Allocation has already created/validated these directories and applied caller
ancestry checks. This helper only checks exact lexical paths and final grant
slots; it performs no filesystem reads or lock acquisition.
"""
from dataclasses import dataclass

from .backends.conversation import ConversationRefused
from . import scratch, shared_scratch

_AUTHORITY = object()


@dataclass(frozen=True)
class ManagedPaths:
    paths: tuple[tuple[str, str], ...]
    authority: object

    def __deepcopy__(self, memo):
        return self  # Immutable owner-local capability; never JSON serializable.

    def roles(self):
        if self.authority is not _AUTHORITY:
            raise ConversationRefused('managed path proof is not owner-local')
        return dict(self.paths)


def from_allocation(layout, task_id, allocation, invocation):
    if allocation is None:
        return ManagedPaths((), _AUTHORITY)
    if not isinstance(allocation, shared_scratch.Allocation):
        raise ConversationRefused('managed allocation requires the actual owner allocation')
    expected = {
        'task': str(scratch.directory(layout.tasks_directory, task_id)),
        'chain': str(shared_scratch.directory(layout.tasks_directory, 'chain', allocation.chain_id)),
    }
    if allocation.group_name is not None:
        expected['group'] = str(shared_scratch.directory(layout.tasks_directory, 'group', allocation.group_name))
    for role in ('task', 'chain', 'group'):
        actual = getattr(allocation, role)
        if actual is not None and actual != expected.get(role):
            raise ConversationRefused('managed allocation does not match its exact owner path')
    roles = {}
    for role, path in (('task', invocation.scratch_directory),
                       *[(role, path) for role in ('chain', 'group')
                         for path in invocation.shared_scratch_directories if path == expected.get(role)]):
        if path is None:
            continue
        if path != expected.get(role) or allocation.access.get(role) != shared_scratch.WRITABLE:
            raise ConversationRefused('managed grant differs from the actual allocation')
        roles[path] = role + '_scratch'
    if len(invocation.shared_scratch_directories) != sum(roles.get(path) in ('chain_scratch', 'group_scratch')
                                                        for path in invocation.shared_scratch_directories):
        raise ConversationRefused('unmapped managed shared grant')
    actual_writable = set(allocation.writable)
    if set(roles) != actual_writable:
        raise ConversationRefused('final managed grants do not match allocation writable slots')
    return ManagedPaths(tuple(roles.items()), _AUTHORITY)


def from_record(layout, record, invocation):
    """Revalidate exact saved allocation only after source binding/receipt checks."""
    if not record.scratch:
        if invocation.scratch_directory or invocation.shared_scratch_directories:
            raise ConversationRefused('saved managed allocation is missing')
        return ManagedPaths((), _AUTHORITY)
    value = record.scratch
    allocation = shared_scratch.Allocation(task=value.get('task'), chain=value.get('chain'), group=value.get('group'),
        chain_id=value.get('chain_id'), group_name=value.get('group_name'), access=dict(value.get('access', {})))
    if value.get('writable') != allocation.writable:
        raise ConversationRefused('saved managed allocation writable slots mismatch')
    return from_allocation(layout, record.task_id, allocation, invocation)
