from polybridge import scratch, retention

def test_task_retention_removes_scratch_without_following_symlinks(tmp_path):
    log=tmp_path/'tasks';log.mkdir();(log/'worker.meta.json').write_text('{}')
    path=scratch.create(log,'worker');(path/'payload').write_text('review')
    assert retention._delete_task_files(log,'worker')
    assert not path.exists()
    outside=tmp_path/'outside';outside.mkdir();(outside/'keep').write_text('safe')
    path.symlink_to(outside,target_is_directory=True)
    assert retention._delete_task_files(log,'worker')
    assert (outside/'keep').read_text()=='safe'
