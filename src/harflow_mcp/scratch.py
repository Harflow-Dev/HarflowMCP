"""Task-owned artifacts retained with their recoverable task records."""
from pathlib import Path
import shutil
from .store import validate_task_id


def directory(log_dir: Path, task_id: str) -> Path:
    validate_task_id(task_id)
    return log_dir.parent / 'scratch' / task_id


def create(log_dir: Path, task_id: str) -> Path:
    path = directory(log_dir, task_id)
    if path.parent.is_symlink() or path.is_symlink():
        raise ValueError('Task scratch directory must not be a symlink')
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    return path.resolve()


def remove(log_dir: Path, task_id: str) -> None:
    path = directory(log_dir, task_id)
    if path.parent.is_symlink():
        raise OSError('Scratch root is a symlink; refusing cleanup')
    if path.is_symlink():
        path.unlink()
    elif path.exists():
        shutil.rmtree(path)
