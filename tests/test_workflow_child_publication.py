"""Exclusive child IDs become visible only after their complete record is flushed."""
import asyncio
import json
import os
import stat
from types import SimpleNamespace

import pytest

from polybridge import workflow_invocation as inv
from polybridge import workflows as w
from test_workflow_run_node_recovery import TreeRegistry, child_runs, seed_parent, store


def reserved_child(store, tmp_path):
    parent = seed_parent(store, tmp_path, 'parent', stage='preparing')
    activation = parent['activations'][0]
    node = next(n for n in parent['definition']['nodes'] if n['id'] == 'call')
    child = inv.child_run_record(store, parent, activation, node, activation['invocation'])
    return parent, child


@pytest.mark.parametrize('failure', ['serialization', 'write', 'file_fsync', 'link'])
def test_failure_before_publication_never_leaves_a_partial_child(store, tmp_path, monkeypatch, failure):
    _, child = reserved_child(store, tmp_path)
    path = store.runs / f"{child['workflow_run_id']}.json"
    with monkeypatch.context() as patch:
        if failure == 'serialization':
            child['unserializable'] = object()
        elif failure == 'write':
            def partial(value, handle, **kwargs):
                handle.write('{"partial":')
                raise OSError('write failed')
            patch.setattr(inv.json, 'dump', partial)
        elif failure == 'file_fsync':
            patch.setattr(inv.os, 'fsync', lambda fd: (_ for _ in ()).throw(OSError('fsync failed')))
        else:
            patch.setattr(inv.os, 'link', lambda *args: (_ for _ in ()).throw(OSError('link failed')))
        with pytest.raises((OSError, TypeError)):
            inv.write_child_run(store, child)
    assert not path.exists()
    assert not list(store.runs.glob(f'.{path.name}.*.tmp'))
    child.pop('unserializable', None)
    inv.write_child_run(store, child)
    assert json.loads(path.read_text()) == child
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_publication_is_exclusive_and_link_observes_complete_flushed_private_record(store, tmp_path, monkeypatch):
    _, child = reserved_child(store, tmp_path)
    path = store.runs / f"{child['workflow_run_id']}.json"
    original_link, original_fsync = os.link, os.fsync
    flushed = []
    def fsync(fd):
        if stat.S_ISREG(os.fstat(fd).st_mode):
            flushed.append(True)
        return original_fsync(fd)
    def link(temp, final):
        assert flushed and not path.exists()
        assert json.loads(temp.read_text()) == child
        assert stat.S_IMODE(temp.stat().st_mode) == 0o600
        return original_link(temp, final)
    with monkeypatch.context() as patch:
        patch.setattr(inv.os, 'fsync', fsync)
        patch.setattr(inv.os, 'link', link)
        inv.write_child_run(store, child)
    before = path.read_bytes()
    with pytest.raises(FileExistsError):
        inv.write_child_run(store, {**child, 'prompt': 'must never replace'})
    assert path.read_bytes() == before
    assert not list(store.runs.glob(f'.{path.name}.*.tmp'))


def test_exclusive_temp_collision_never_removes_another_writers_file(store, tmp_path, monkeypatch):
    _, child = reserved_child(store, tmp_path)
    path = store.runs / f"{child['workflow_run_id']}.json"
    temp = path.with_name(f'.{path.name}.collision.tmp')
    temp.write_bytes(b'another writer owns this temporary file')
    monkeypatch.setattr(inv.uuid, 'uuid4', lambda: SimpleNamespace(hex='collision'))
    with pytest.raises(FileExistsError):
        inv.write_child_run(store, child)
    assert temp.read_bytes() == b'another writer owns this temporary file'
    assert not path.exists()


async def test_postpublication_directory_fsync_failure_recovers_same_child(store, tmp_path, monkeypatch):
    parent, child = reserved_child(store, tmp_path)
    original_fsync = os.fsync
    def fsync(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError('directory fsync failed after publication')
        return original_fsync(fd)
    with monkeypatch.context() as patch:
        patch.setattr(inv.os, 'fsync', fsync)
        with pytest.raises(OSError, match='directory fsync'):
            inv.write_child_run(store, child)
    assert store.get_run(child['workflow_run_id'])['parent_link'] == child['parent_link']
    registry = TreeRegistry(store.root)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, store).execute(parent['workflow_run_id']), 15)
    final = store.get_run(parent['workflow_run_id'])
    assert final['status'] == 'completed', final.get('attention_reason')
    assert [c['workflow_run_id'] for c in child_runs(store, final)] == [child['workflow_run_id']]
    assert len([call for call in registry.dispatches if call['label'] == 'work']) == 1


@pytest.mark.parametrize('matching_owner', [True, False])
async def test_concurrent_exclusive_winner_is_adopted_only_for_same_invocation(store, tmp_path, monkeypatch, matching_owner):
    parent, child = reserved_child(store, tmp_path)
    original_link = os.link
    winner = {**child, 'prompt': 'concurrent winner'}
    winner['parent_link'] = dict(child['parent_link'])
    if not matching_owner:
        winner['parent_link']['execution_id'] = 'another-invocation'
    def link(temp, final):
        # The competing publisher must retain the durable selected binding.
        winner['child_session_selection'] = json.loads(temp.read_text())['child_session_selection']
        final.write_text(json.dumps(winner), encoding='utf-8')
        os.chmod(final, 0o600)
        return original_link(temp, final)
    registry = TreeRegistry(store.root)
    with monkeypatch.context() as patch:
        patch.setattr(inv.os, 'link', link)
        await asyncio.wait_for(w.WorkflowSupervisor(registry, store).execute(parent['workflow_run_id']), 15)
    final = store.get_run(parent['workflow_run_id'])
    assert final['status'] == ('completed' if matching_owner else 'needs_attention')
    actual = store.get_run(child['workflow_run_id'])
    assert actual['prompt'] == 'concurrent winner'
    assert actual['parent_link'] == winner['parent_link']
    assert len([call for call in registry.dispatches if call['label'] == 'work']) == int(matching_owner)
