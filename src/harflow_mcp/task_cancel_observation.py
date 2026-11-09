"""Ctl-only cancellation evidence; never an authority or settlement writer."""
from copy import deepcopy
from datetime import datetime, timezone
import json

from . import identity, store
from .observation_budget import Budget, current

MAX_ITEMS = 100
MAX_BYTES = 65536


def fallback():
    return dict(scope='recorded_tasks_and_pending_launches',
                observed_at=datetime.now(timezone.utc).isoformat(),
                coverage='unknown', truncated=False, items=[])


def collect(collector, method, *args):
    """Collection is bookkeeping even when an injected/private collector fails."""
    if collector is None:
        return
    try:
        return getattr(collector, method)(*args)
    except Exception:
        try:
            collector.failed = True
        except Exception:
            pass


class Collector:
    def __init__(self, task_id):
        self.task_id = task_id
        self.targets = {task_id}
        self.covered = set()
        self.pending = {}
        self.pending_requested = set()
        self.settled = {}
        self.withdrawal_unsettled = set()
        self.unconverged = set()
        self.refused_documents = []
        self.incomplete = False
        self.failed = False

    def discover(self, targets):
        self.targets.update(targets)
        refusals = getattr(targets, 'refusals', {})
        self.incomplete |= bool(refusals)
        self.refused_documents.extend(refusals.values())

    def processed(self, targets):
        self.covered.update(targets)

    def evidence(self, result):
        self.incomplete |= any(bool(result.get(key)) for key in (
            'cascade_incomplete', 'history_incomplete', 'unconverged',
            'refused_tasks', 'launch_refusals', 'withdrawal_unsettled'))
        self.pending_requested.update(result.get('pending_cancel_requested', ()))
        self.withdrawal_unsettled.update(result.get('withdrawal_unsettled', ()))
        self.unconverged.update(result.get('unconverged', ()))
        self.refused_documents.extend(result.get('refused_tasks', ()))
        self.refused_documents.extend(result.get('launch_refusals', ()))

    def receipt(self, task_id, receipt):
        if receipt is not None:
            self.targets.add(task_id)
            binding = deepcopy({key: receipt.get(key) for key in (
                'task_id', 'generation', 'request_sha256', 'configuration_sha256',
                'root', 'owner', 'directory_binding', 'directory_identity')})
            if self.pending.get(task_id) != binding:
                self.settled.pop(task_id, None)
            self.pending[task_id] = binding
            return deepcopy(binding)

    def settlement(self, task_id, binding):
        if binding is not None and binding == self.pending.get(task_id):
            self.settled[task_id] = deepcopy(binding)

    def project(self, log_dir):
        from .task_supervision import read_launch, read_task_record
        result = fallback()
        result['coverage'] = 'complete'
        ids = [self.task_id, *sorted(self.targets - {self.task_id})]
        result['truncated'] = len(ids) > MAX_ITEMS
        incomplete = self.incomplete or result['truncated']
        budget = Budget()
        token = current.set(budget)
        try:
            for task_id in ids[:MAX_ITEMS]:
                row = dict(task_id=task_id, state='unknown', basis='insufficient_evidence', status=None)
                try:
                    budget.remaining()
                    record = read_task_record(log_dir, task_id, include_prompt=False)
                    if record is not None:
                        resolved = []
                        status = store.resolve_status(log_dir, record, detail=False, _observer_record=resolved)[0]
                        qualified = resolved[0]
                        source = getattr(qualified, '_source_identity', None)
                        if source is None or getattr(record, '_source_identity', None) != source:
                            raise ValueError('Metadata replaced during status resolution')
                        row['status'] = status
                        verdict = identity.identity_check(identity.task_identity(
                            qualified.pid, qualified.start_time, qualified.markers))
                        try:
                            fresh = read_task_record(log_dir, task_id, include_prompt=False)
                            if fresh is None or getattr(fresh, '_source_identity', None) != source:
                                raise ValueError('Metadata replaced during identity observation')
                        except Exception:
                            row['status'] = None
                            raise
                        if verdict == 'alive':
                            row.update(state='running', basis='confirmed_live_identity')
                        elif (verdict == 'dead' and qualified.start_time is not None
                              and type(qualified.pid) is int and qualified.pid > 0
                              and qualified.exit_code is not None and qualified.output_drained is True):
                            row.update(state='stopped', basis='observed_settlement')
                    else:
                        receipt = read_launch(log_dir, task_id)
                        binding = self.pending.get(task_id)
                        if receipt is None or binding is None or any(receipt.get(k) != v for k, v in binding.items()):
                            raise ValueError('Pending binding unavailable')
                        if read_task_record(log_dir, task_id, include_prompt=False) is not None:
                            raise ValueError('Launch spawned during observation')
                        if receipt['state'] == 'not_started' and self.settled.get(task_id) == binding:
                            row.update(state='stopped', basis='proven_not_started',
                                       status='cancelled' if receipt.get('not_started_reason') == 'cancelled' else 'failed')
                        elif receipt['state'] in ('waiting_for_resources', 'granted', 'reserved', 'claimed'):
                            row['status'] = 'running'
                        else:
                            incomplete = True
                except Exception:
                    incomplete = True
                result['items'].append(row)
            result['coverage'] = 'incomplete' if incomplete else 'complete'
            while len(json.dumps(result, allow_nan=False).encode('utf-8')) > MAX_BYTES:
                result['items'].pop()
                result.update(truncated=True, coverage='incomplete')
            return result
        finally:
            current.reset(token)


def finish(collector, log_dir, *, _fallback=None):
    result = _fallback if _fallback is not None else fallback()
    try:
        if collector is not None and not collector.failed:
            candidate = collector.project(log_dir)
            json.dumps(candidate, allow_nan=False).encode('utf-8')
            return candidate
    except Exception:
        pass
    return result
