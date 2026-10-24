"""Read-only task handles for an independently owned execution."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
import weakref

from . import store


class TaskObservationUnavailable(RuntimeError):
    """The agent and its owner are confirmed gone without an observed terminal receipt."""


class AttachedDone:
    def __init__(self, task):
        self.task = task

    def is_set(self):
        record = self.task.record()
        return bool(record.status in store.TERMINAL_RECORD_STATUSES and record.exit_code is not None and record.finished_at)

    async def wait(self):
        from . import identity
        loop = asyncio.get_running_loop()
        next_owner_check = 0.0
        def unavailable(record):
            return (identity.identity_check(record.owner) == 'dead'
                    and identity.identity_check(identity.task_identity(record.pid, record.start_time, record.markers)) == 'dead')
        while not self.is_set():
            if loop.time() >= next_owner_check:
                record = self.task.record()
                lost = await asyncio.to_thread(unavailable, record)
                next_owner_check = loop.time() + 1.0
                # The owner may have published its receipt immediately before exiting.
                if lost and not self.is_set():
                    raise TaskObservationUnavailable(f'Task {record.task_id} lost its owner and process without an observed terminal receipt')
            await asyncio.sleep(0.05)
        return True


class AttachedTask:
    """A durable view, never an owner and never a source for metadata writes."""
    def __init__(self, log_dir: Path, task_id: str, *, registry=None):
        object.__setattr__(self, 'log_dir', log_dir)
        object.__setattr__(self, 'task_id', store.validate_task_id(task_id))
        object.__setattr__(self, 'done', AttachedDone(self))
        object.__setattr__(self, '_registry', weakref.ref(registry) if registry is not None else None)
        object.__setattr__(self, '_terminal_observed', False)
        object.__setattr__(self, '_observer_notices', ())

    def __setattr__(self, name, value):
        raise TypeError('Attached task handles are read-only; mutate through the owning registry')

    def record(self):
        from .task_supervision import read_task_record
        record = read_task_record(self.log_dir, self.task_id)
        if record is None:
            raise ValueError(f'Task metadata unavailable: {self.task_id}')
        if (not self._terminal_observed and record.status in store.TERMINAL_RECORD_STATUSES
                and record.exit_code is not None and record.finished_at):
            object.__setattr__(self, '_terminal_observed', True)
            registry = self._registry() if self._registry is not None else None
            if registry is not None:
                registry.prune()
        return record

    def _view(self, reader):
        # Snapshot and brief perform their own authoritative metadata reads. Restart the whole
        # observation only when the reader positively identifies an atomic source replacement.
        for attempt in range(3):
            try:
                return reader(self.log_dir, self.record())
            except store.TaskDocumentError as exc:
                if not getattr(exc, '_source_changed_during_read', False) or attempt == 2:
                    raise

    def _add_observer_notice(self, text):
        if text not in self._observer_notices:
            object.__setattr__(self, '_observer_notices', (*self._observer_notices, text)[-8:])

    def _with_observer_notices(self, payload):
        if self._observer_notices:
            payload['notices'] = [*payload.get('notices', []), *self._observer_notices]
        return payload

    def brief(self):
        return self._with_observer_notices(self._view(store.brief))

    def snapshot(self):
        from .attached_replay import replay
        return self._with_observer_notices(self._view(lambda directory, record: store.snapshot(directory, record, _replay_provider=replay)))

    @property
    def acc(self):
        from .attached_replay import replay
        return replay(self.log_dir, self.task_id, self.record().backend)[0]

    @property
    def finished(self):
        return self.done.is_set()

    @property
    def duration_seconds(self):
        record = self.record()
        end = datetime.fromisoformat(record.finished_at) if record.finished_at else datetime.now(timezone.utc)
        return round((end - datetime.fromisoformat(record.started_at)).total_seconds(), 3)

    @property
    def proc(self):
        return None

    def __getattr__(self, name):
        record = self.record()
        if name == 'started_at':
            return datetime.fromisoformat(record.started_at)
        if name == 'repo_path':
            return Path(record.repo_path)
        if name == 'log_path':
            return store.log_path(self.log_dir, self.task_id)
        if name == 'status':
            return self.snapshot()['status']
        if name == 'cancel_requested':
            return record.status == 'cancelled'
        if name in {'latest_record_landed', 'record_landed'}:
            return True
        if name in {'persistence_error', 'monitor', 'events', 'pump', 'termination'}:
            return None
        if name == 'inbox_closed':
            from . import inbox
            return inbox.is_closed(self.log_dir, self.task_id)
        return getattr(record, name)
