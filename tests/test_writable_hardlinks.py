import os
from pathlib import Path

import pytest
from polybridge import runtime_paths


def test_existing_aliases_and_single_links(tmp_path):
    outside = tmp_path / 'outside'
    outside.write_text('source')
    root = tmp_path / 'grant'
    root.mkdir()
    regular = root / 'regular'
    regular.write_text('safe')
    runtime_paths.assert_single_link_writable_files([(root, 'directory'), (regular, 'file')])
    alias = root / 'alias'
    os.link(outside, alias)
    for scope in [(root, 'directory'), (alias, 'file')]:
        with pytest.raises(ValueError, match='multiply-linked'):
            runtime_paths.assert_single_link_writable_files([scope])


def test_does_not_follow_outside_directory_symlink(tmp_path):
    outside = tmp_path / 'outside'
    outside.mkdir()
    source = outside / 'source'
    source.write_text('x')
    os.link(source, outside / 'alias')
    root = tmp_path / 'grant'
    root.mkdir()
    (root / 'escape').symlink_to(outside, target_is_directory=True)
    runtime_paths.assert_single_link_writable_files([(root, 'directory')])
    # In-scope symlink targets are still visited through their direct path.
    inside = root / 'inside'
    inside.mkdir()
    os.link(source, inside / 'alias')
    (root / 'indirect').symlink_to(inside, target_is_directory=True)
    with pytest.raises(ValueError, match='multiply-linked'):
        runtime_paths.assert_single_link_writable_files([(root, 'directory')])


@pytest.mark.parametrize('bound', ['WRITABLE_SCAN_MAX_ENTRIES', 'WRITABLE_SCAN_MAX_DEPTH', 'WRITABLE_SCAN_SECONDS'])
def test_traversal_bounds_fail_closed(tmp_path, monkeypatch, bound):
    (tmp_path / 'child').mkdir()
    monkeypatch.setattr(runtime_paths, bound, 0)
    with pytest.raises(ValueError, match='bound|budget'):
        runtime_paths.assert_single_link_writable_files([(tmp_path, 'directory')])


def test_io_error_fails_closed(tmp_path, monkeypatch):
    def denied(*args, **kwargs):
        raise PermissionError('denied')
    monkeypatch.setattr(runtime_paths.os, 'scandir', denied)
    with pytest.raises(ValueError, match='accessible'):
        runtime_paths.assert_single_link_writable_files([(tmp_path, 'directory')])


def test_overlapping_scopes_are_not_rescanned(tmp_path, monkeypatch):
    child = tmp_path / 'file'
    child.write_text('safe')
    monkeypatch.setattr(runtime_paths, 'WRITABLE_SCAN_MAX_ENTRIES', 2)
    runtime_paths.assert_single_link_writable_files([
        (tmp_path, 'directory'), (tmp_path, 'directory'), (child, 'file')])


def test_directory_identity_change_fails_closed(tmp_path, monkeypatch):
    from types import SimpleNamespace
    actual = os.fstat
    def changed(fd):
        info = actual(fd)
        return SimpleNamespace(st_dev=info.st_dev, st_ino=info.st_ino + 1)
    monkeypatch.setattr(runtime_paths.os, 'fstat', changed)
    with pytest.raises(ValueError, match='identity changed'):
        runtime_paths.assert_single_link_writable_files([(tmp_path, 'directory')])


def test_renamed_open_parent_and_symlink_replacement_fail_closed(tmp_path, monkeypatch):
    root = tmp_path / 'grant'
    root.mkdir()
    (root / 'file').write_text('safe')
    outside = tmp_path / 'outside'
    outside.mkdir()
    (outside / 'file').write_text('replacement')
    actual = os.scandir
    replaced = False
    def redirect(fd):
        nonlocal replaced
        if not replaced:
            replaced = True
            root.rename(tmp_path / 'renamed')
            root.symlink_to(outside, target_is_directory=True)
        return actual(fd)
    monkeypatch.setattr(runtime_paths.os, 'scandir', redirect)
    with pytest.raises(ValueError, match='identity changed'):
        runtime_paths.assert_single_link_writable_files([(root, 'directory')])


@pytest.mark.parametrize('kind', ['file', 'directory'])
def test_declared_root_symlink_rejected(tmp_path, kind):
    target = tmp_path / 'target'
    target.mkdir() if kind == 'directory' else target.write_text('safe')
    alias = tmp_path / 'alias'
    alias.symlink_to(target, target_is_directory=kind == 'directory')
    with pytest.raises(ValueError, match='scope type changed'):
        runtime_paths.assert_single_link_writable_files([(alias, kind)])


def test_transient_parent_redirect_restored_before_final_check(tmp_path, monkeypatch):
    from contextlib import contextmanager
    root = tmp_path / 'grant'
    root.mkdir()
    sub = root / 'sub'
    sub.mkdir()
    source = tmp_path / 'source.py'
    source.write_text('outside source')
    os.link(source, sub / 'alias.py')
    benign = tmp_path / 'benign'
    benign.mkdir()
    (benign / 'alias.py').write_text('single link')
    moved = tmp_path / 'temporarily-moved'
    actual = os.scandir
    inode = sub.stat().st_ino

    @contextmanager
    def redirect(fd):
        # Enumeration remains anchored to the original directory, while absolute
        # child metadata would see the benign replacement. Restore before final
        # path checks so only descriptor-relative metadata catches the attack.
        hit = os.fstat(fd).st_ino == inode
        if hit:
            sub.rename(moved)
            sub.symlink_to(benign, target_is_directory=True)
        try:
            with actual(fd) as iterator:
                yield iterator
        finally:
            if hit:
                sub.unlink()
                moved.rename(sub)

    monkeypatch.setattr(runtime_paths.os, 'scandir', redirect)
    with pytest.raises(ValueError, match='multiply-linked'):
        runtime_paths.assert_single_link_writable_files([(root, 'directory')])
    assert not sub.is_symlink()
    assert (sub / 'alias.py').stat().st_nlink == 2
