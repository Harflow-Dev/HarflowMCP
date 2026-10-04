"""A narrowly scoped GitHub COMMENT-review publisher for publish-capable tasks."""
from pathlib import Path
import json
import re
import subprocess
from . import scratch, workflow_hooks, workflows


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Duplicate review payload field: ' + key)
        result[key] = value
    return result


def publish_review(log_dir: Path, record, pr: int, input_file: str) -> dict:
    if record.freedom not in {'publish', 'unrestricted'} or record.network is False:
        raise ValueError('Publishing requires saved publish access and network permission')
    if record.status != 'running':
        raise ValueError('Only the currently running task can publish')
    association = workflow_hooks.owner(log_dir, record.task_id, strict=True)
    if association is not None:
        run = workflows.WorkflowStore(log_dir.parent).get_run(association['workflow_run_id'])
        if Path(record.repo_path).resolve() != Path(run['repo_path']).resolve():
            raise ValueError('Task repository does not match its workflow')
        node = next((n for n in run['definition']['nodes'] if n['id'] == association['node_id']), None)
        if association['role'] != 'node' or node is None or workflows.run_effective_freedom(run, node) not in {'publish', 'unrestricted'}:
            raise ValueError('Only a publish-capable worker may publish a review')
    if isinstance(pr, bool) or not isinstance(pr, int) or pr < 1:
        raise ValueError('PR number must be positive')
    repo = Path(record.repo_path).resolve()
    source = Path(input_file).resolve()
    allowed = (repo, scratch.directory(log_dir, record.task_id).resolve())
    if not any(source.is_relative_to(root) for root in allowed):
        raise ValueError('Payload must be in this task workspace or scratch directory')
    body = source.read_bytes()
    if len(body) > 16 * 1024 * 1024:
        raise ValueError('Review payload exceeds 16 MiB')
    payload = json.loads(body, object_pairs_hook=_unique_object)
    if not isinstance(payload, dict) or set(payload) - {'body','event','comments','commit_id'}:
        raise ValueError('Unsupported review payload fields')
    if payload.get('event') != 'COMMENT':
        raise ValueError('Only COMMENT reviews are supported; approval verdicts are refused')
    if not isinstance(payload.get('body', ''), str) or not isinstance(payload.get('comments', []), list):
        raise ValueError('Review body and comments have invalid types')
    if not payload.get('body', '').strip() and not payload.get('comments'):
        raise ValueError('Review needs a body or inline comments')
    if 'commit_id' in payload and (not isinstance(payload['commit_id'],str) or not re.fullmatch(r'[0-9a-fA-F]{40}',payload['commit_id'])):
        raise ValueError('commit_id must be a complete commit SHA')
    for comment in payload.get('comments', []):
        if not isinstance(comment,dict) or set(comment)-{'path','body','line','side','start_line','start_side','position','subject_type'}:
            raise ValueError('Unsupported inline comment fields')
        if not isinstance(comment.get('body'),str) or not comment['body'].strip() or not isinstance(comment.get('path'),str):
            raise ValueError('Inline comments require a path and body')
        path=Path(comment['path'])
        if path.is_absolute() or '..' in path.parts:
            raise ValueError('Inline comment path must be relative')
        for key in ('line','start_line','position'):
            if key in comment and (type(comment[key]) is not int or comment[key] < 1):
                raise ValueError('Inline positions must be positive integers')
        for key in ('side','start_side'):
            if key in comment and comment[key] not in {'LEFT','RIGHT'}:
                raise ValueError('Invalid inline side')
    endpoint = 'repos/' + _repository(repo) + '/pulls/' + str(pr) + '/reviews'
    # Feed the validated bytes, not a re-openable path, and never compose a shell command.
    outcome = subprocess.run(['gh','api',endpoint,'--method','POST','--input','-'],cwd=repo,input=body,capture_output=True,check=True)
    value = json.loads(outcome.stdout)
    return {'event':'COMMENT','review_id':value.get('id'),'url':value.get('html_url')}


def _repository(repo: Path) -> str:
    remote = subprocess.run(['git', 'remote', 'get-url', 'origin'], cwd=repo, capture_output=True, text=True, check=True).stdout.strip()
    match = re.fullmatch(r'(?:git@github\.com:|https://github\.com/|ssh://git@github\.com/)([A-Za-z0-9][A-Za-z0-9-]*/[A-Za-z0-9_.-]+?)(?:\.git)?', remote)
    if not match or match[1].split('/')[1] in {'.', '..'}:
        raise ValueError('GitHub access requires an unambiguous github.com origin remote')
    return match[1]


def github_read(record, resource: str, pr: int | None = None):
    """Fixed GET resources; never accept arbitrary endpoints, verbs, or gh options."""
    if record.status != 'running' or record.network is False:
        raise ValueError('GitHub reads require a running task with network permitted')
    if resource not in {'user', 'pr', 'reviews', 'comments'}:
        raise ValueError('Unsupported GitHub read resource')
    repo = Path(record.repo_path).resolve()
    if resource == 'user':
        if pr is not None:
            raise ValueError('User resource does not take a PR number')
        endpoint = 'user'
    else:
        if type(pr) is not int or pr < 1:
            raise ValueError('PR number must be positive')
        endpoint = 'repos/' + _repository(repo) + '/pulls/' + str(pr)
        if resource != 'pr':
            endpoint += '/' + resource
    response = subprocess.run(['gh', 'api', endpoint, '--method', 'GET', '--paginate', '--slurp'], cwd=repo, capture_output=True, check=True)
    pages = json.loads(response.stdout)
    if not isinstance(pages, list):
        raise ValueError('Unexpected GitHub response format')
    return [item for page in pages for item in page] if resource in {'reviews', 'comments'} else (pages[0] if len(pages) == 1 else pages)
