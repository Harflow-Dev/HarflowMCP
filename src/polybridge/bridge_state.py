"""Keep bounded worker writes outside bridge authorization and control state."""
from pathlib import Path


def assert_state_outside_writes(log_dir: Path, repo: Path, grants, scratch_directory: str | None = None) -> None:
    """Protect default CLI state and this registry's custom state before execution.

    Only the separately allocated task scratch path is writable within state.
    Caller-supplied grants and repositories never receive that exception.
    """
    from .runtime_paths import _chain
    roots = {Path.home() / '.polybridge', Path(log_dir).absolute().parent}
    scopes = [(Path(repo).resolve(strict=True), 'directory')]
    scopes.extend((Path(grant.path), grant.kind) for grant in grants)
    protected = []
    controls = []
    for root in roots:
        chain, target = _chain(root)
        protected.append((root.absolute(), target, chain))
        for name in ('tasks', 'sessions', 'workflows', 'workflow-runs', 'workflow-leases', 'workflow-owners', 'notifications.sqlite3', 'ctl.log'):
            child = root / name
            child_chain, child_target = _chain(child)
            protected.append((child.absolute(), child_target, child_chain))
            controls.append(child_target)
    # The task directory may have a separate alias/target outside its parent.
    task_chain, task_target = _chain(Path(log_dir))
    protected.append((Path(log_dir).absolute(), task_target, task_chain))
    for scope, kind in scopes:
        for lexical, target, chain in protected:
            if (scope.is_relative_to(lexical) or scope.is_relative_to(target)
                    or any(scope == path or (kind == 'directory' and path.is_relative_to(scope)) for path in chain)):
                raise ValueError('writable authority overlaps protected bridge task/control state')
    if scratch_directory is not None:
        allocated = Path(scratch_directory)
        expected = Path(log_dir).parent / 'scratch'
        if (expected.is_symlink() or allocated.parent != expected.resolve(strict=True)
                or allocated.resolve(strict=True) != allocated or allocated.is_symlink()):
            raise ValueError('task scratch identity is outside its allocated namespace')
        # Scratch must never resolve into the task-record tree or another state
        # root's controls via a substituted parent alias.
        if any(allocated.is_relative_to(target) for target in (*controls, task_target)):
            raise ValueError('task scratch overlaps bridge task/control state')
