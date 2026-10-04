import json
from types import SimpleNamespace
from pathlib import Path
import pytest
from polybridge import publication, scratch, retention, workflow_hooks


@pytest.fixture
def publish_context(tmp_path,monkeypatch):
    log=tmp_path/'tasks';log.mkdir();repo=tmp_path/'repo';repo.mkdir()
    record=SimpleNamespace(task_id='worker',freedom='publish',network=None,status='running',repo_path=str(repo))
    monkeypatch.setattr(workflow_hooks,'owner',lambda *a,**k:None)
    calls=[]
    def execute(argv,**kw):
        calls.append((argv,kw))
        return SimpleNamespace(stdout='git@github.com:owner/repo.git\n' if argv[0]=='git' else b'{"id":3,"html_url":"https://github.com/owner/repo/pull/2#review"}')
    monkeypatch.setattr(publication.subprocess,'run',execute)
    path=scratch.create(log,'worker')/'payload.json';path.write_text(json.dumps({'event':'COMMENT','body':'Reviewed','comments':[{'path':'file.py','body':'Finding','line':2,'side':'RIGHT'}]}))
    return log,record,path,calls


def test_publish_helper_posts_validated_complete_bytes_without_shell(publish_context):
    log,record,path,calls=publish_context;body=path.read_bytes()
    result=publication.publish_review(log,record,2,str(path))
    assert result['event']=='COMMENT'
    assert calls[-1][0]==['gh','api','repos/owner/repo/pulls/2/reviews','--method','POST','--input','-']
    assert calls[-1][1]['input']==body
    assert 'shell' not in calls[-1][1]


@pytest.mark.parametrize('change', ['approval','delete','foreign','readonly','network','settled','remote'])
def test_publish_helper_refuses_unscoped_or_destructive_requests(publish_context,tmp_path,change):
    log,record,path,calls=publish_context
    if change=='approval':path.write_text('{"event":"APPROVE","body":"approve"}')
    elif change=='delete':path.write_text('{"event":"COMMENT","body":"hi","method":"DELETE"}')
    elif change=='foreign':path=tmp_path/'foreign.json';path.write_text('{"event":"COMMENT","body":"hi"}')
    elif change=='readonly':record.freedom='read_only'
    elif change=='network':record.network=False
    elif change=='settled':record.status='completed'
    elif change=='remote':publication.subprocess.run=lambda *a,**k:SimpleNamespace(stdout='https://evil.test/owner/repo.git')
    with pytest.raises(ValueError):publication.publish_review(log,record,2,str(path))
    assert not any(argv[0]=='gh' for argv,kw in calls)


def test_task_retention_removes_scratch_without_following_symlinks(tmp_path):
    log=tmp_path/'tasks';log.mkdir();(log/'worker.meta.json').write_text('{}')
    path=scratch.create(log,'worker');(path/'payload').write_text('review')
    assert retention._delete_task_files(log,'worker')
    assert not path.exists()
    outside=tmp_path/'outside';outside.mkdir();(outside/'keep').write_text('safe')
    path.symlink_to(outside,target_is_directory=True)
    assert retention._delete_task_files(log,'worker')
    assert (outside/'keep').read_text()=='safe'


@pytest.mark.parametrize('body', [
    '{"event":"APPROVE","event":"COMMENT","body":"hi"}',
    '{"event":"COMMENT","body":"hi","comments":[{"path":"a","body":"b","body":"c"}]}',
])
def test_publish_refuses_duplicate_json_fields(publish_context, body):
    log, record, path, calls = publish_context
    path.write_text(body)
    with pytest.raises(ValueError, match='Duplicate'):
        publication.publish_review(log, record, 2, str(path))
    assert not any(argv[0] == 'gh' for argv, _ in calls)


def test_github_read_uses_fixed_get_and_preserves_all_pages(publish_context, monkeypatch):
    _, record, _, calls = publish_context
    record.freedom = 'read_only'
    original = publication.subprocess.run
    def execute(argv, **kwargs):
        if argv[0] == 'git':
            return original(argv, **kwargs)
        calls.append((argv, kwargs))
        return SimpleNamespace(stdout=b'[[{"id":1}],[{"id":2}]]')
    monkeypatch.setattr(publication.subprocess, 'run', execute)
    assert publication.github_read(record, 'reviews', 2) == [{'id':1}, {'id':2}]
    assert calls[-1][0] == ['gh','api','repos/owner/repo/pulls/2/reviews','--method','GET','--paginate','--slurp']


@pytest.mark.parametrize('resource,pr', [('DELETE',2), ('../../repos',2), ('reviews',0), ('user',2)])
def test_github_read_rejects_unscoped_requests(publish_context, resource, pr):
    _, record, _, calls = publish_context
    with pytest.raises(ValueError):
        publication.github_read(record, resource, pr)
    assert not any(argv[0] == 'gh' for argv, _ in calls)
