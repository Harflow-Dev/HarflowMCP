"""Task-owned artifacts retained with their recoverable task records."""

from .state_access import task_operation
from pathlib import Path
import shutil
from .store import validate_task_id


def directory(log_dir: Path, task_id: str) -> Path:
    validate_task_id(task_id)
    return log_dir.parent / 'scratch' / task_id


@task_operation
def create(log_dir: Path, task_id: str) -> Path:
    path = directory(log_dir, task_id)
    if path.parent.is_symlink() or path.is_symlink():
        raise ValueError('Task scratch directory must not be a symlink')
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    return path.resolve()


@task_operation
def remove(log_dir: Path, task_id: str) -> None:
    path = directory(log_dir, task_id)
    outputs = output_directory(log_dir, task_id)
    if outputs.parent.is_symlink():
        raise OSError('Output root is a symlink; refusing cleanup')
    if outputs.is_symlink():
        outputs.unlink()
    elif outputs.exists():
        shutil.rmtree(outputs)
    if path.parent.is_symlink():
        raise OSError('Scratch root is a symlink; refusing cleanup')
    if path.is_symlink():
        path.unlink()
    elif path.exists():
        shutil.rmtree(path)


def output_directory(log_dir: Path, task_id: str) -> Path:
    """Engine-owned outputs live outside agent-writable scratch."""
    validate_task_id(task_id)
    return log_dir.parent / 'outputs' / task_id


@task_operation
def materialize_outputs(log_dir: Path, task_id: str, summary: str) -> list[Path]:
    """Publish bounded final output once, without following links or overwriting files.

    Caller titles and filenames are deliberately not accepted. Every resume has a
    fresh task ID. Existing files are refused rather than silently replaced.
    """
    import json
    import os
    if not isinstance(summary, str) or not summary.strip():
        return []
    raw = summary.encode('utf-8')
    if len(raw) > 16 * 1024 * 1024:
        raise ValueError('Task output exceeds the 16 MiB materialization limit')
    directory = output_directory(log_dir, task_id)
    if any(p.is_symlink() for p in (directory, *directory.parents)):
        raise ValueError('Engine output directory must not contain symlinks')
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    contents = {f'report-{task_id}.md': summary}
    try:
        decoded = json.loads(summary)
        plan = decoded.get('result', {}).get('technical_plan') if isinstance(decoded, dict) else None
        if isinstance(plan, str) and plan.strip():
            contents[f'plan-{task_id}.md'] = plan
    except (ValueError, AttributeError):
        pass
    result = []
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for name, content in contents.items():
            import uuid
            temporary = f'.{name}-{uuid.uuid4().hex}.tmp'
            try:
                fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=descriptor)
                with os.fdopen(fd, 'wb') as output:
                    output.write(content.encode('utf-8'))
                    output.flush()
                    os.fsync(output.fileno())
                # Atomic publication with no replacement, including a malicious symlink.
                os.link(temporary, name, src_dir_fd=descriptor, dst_dir_fd=descriptor, follow_symlinks=False)
                result.append(directory / name)
            finally:
                try:
                    os.unlink(temporary, dir_fd=descriptor)
                except FileNotFoundError:
                    pass
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return result
