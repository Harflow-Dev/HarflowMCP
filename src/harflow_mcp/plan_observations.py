"""Bounded best-effort plan metadata processing, independent of the stdout drain."""
from __future__ import annotations

import asyncio
import json
from typing import Callable

from .state_ownership import StateOwnershipError

MAX_FRAMES = 128
MAX_BYTES = 512 * 1024
FINISH_SECONDS = 2.0


class PlanObservations:
    """A task-local worker. Neither queue admission nor closing waits on filesystem I/O."""

    def __init__(self, backend, repo, log_dir, task_id: str, notice: Callable[[str], None]):
        self.backend, self.repo = backend, repo
        self.log_dir, self.task_id, self.notice = log_dir, task_id, notice
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_FRAMES)
        self.bytes = 0
        self.state: dict = {}
        self.worker: asyncio.Task | None = None
        self.closed = False
        self.notices: set[str] = set()

    def warn(self, text: str) -> None:
        if text not in self.notices:
            self.notices.add(text)
            self.notice(text)

    def enqueue(self, event: dict) -> None:
        if self.closed:
            return
        extract = getattr(self.backend, 'plan_file_candidates', None)
        if extract is None:
            return
        frames = extract(event)
        if len(frames) >= 100:
            self.warn('Built-in plan candidate limit reached; artifact tracking may be incomplete.')
        for frame in frames:
            size = len(json.dumps(frame, ensure_ascii=False).encode('utf-8'))
            if self.queue.full() or self.bytes + size > MAX_BYTES:
                self.warn('Built-in plan observation queue is full; artifact tracking is incomplete.')
                continue
            self.bytes += size
            self.queue.put_nowait((frame, size))
            if self.worker is None:
                self.worker = asyncio.create_task(self._run(), name=f'plan-artifacts-{self.task_id}')

    def _process(self, frame: dict) -> None:
        from . import artifacts
        observe = getattr(self.backend, 'observe_plan_candidates', None)
        if observe is None:
            return
        references = observe((frame,), self.state, self.repo)
        for reference in references:
            artifacts.record_backend_plan(self.log_dir, self.task_id, reference)

    async def _run(self) -> None:
        while True:
            frame, size = await self.queue.get()
            try:
                await asyncio.to_thread(self._process, frame)
            except StateOwnershipError:
                raise
            except Exception:
                self.warn('Built-in plan artifact observation failed; artifact tracking is incomplete.')
            finally:
                self.bytes -= size
                self.queue.task_done()

    async def finish(self, timeout: float = FINISH_SECONDS) -> None:
        self.closed = True
        if self.worker is None:
            return
        join = asyncio.create_task(self.queue.join())
        try:
            done, _ = await asyncio.wait({join, self.worker}, timeout=timeout,
                                         return_when=asyncio.FIRST_COMPLETED)
            if self.worker in done:
                # Preserve state-ownership refusals, independently of ordinary observation errors.
                self.worker.result()
            elif join not in done:
                self.warn('Built-in plan artifact tracking exceeded its settlement budget; tracking is incomplete. An in-flight metadata write may still finish.')
        finally:
            join.cancel()
            self.worker.cancel()
            await asyncio.gather(join, self.worker, return_exceptions=True)
            # An in-flight to_thread call retains its own store lease until it returns. No
            # further queued work starts, and late metadata never changes the task outcome.
            while not self.queue.empty():
                _, size = self.queue.get_nowait()
                self.bytes -= size
                self.queue.task_done()
