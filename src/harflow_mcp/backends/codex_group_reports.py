"""Measured Codex native read-only profile with one group-report write root.

Receipts are request-local and rechecked at execution. Neither argv nor a persisted
version string creates authority. No legacy sandbox selectors may be loaded.
"""
from dataclasses import replace
import json
from pathlib import Path
import shutil
import stat
from types import SimpleNamespace

from .base import UnsupportedCapability

FEATURE = 'group_report_output'
PROFILE = 'harflow_group_report'
VERSION = '0.165.0'
FIELDS = ('group_report_directory', 'group_report_identity', 'group_report_runtime',
          'group_report_configuration', 'group_report_qualification')


def support():
    return {'supported': True, 'versions': [VERSION], 'platforms': ['darwin'],
            'freedoms': ['read_only'], 'network': [False], 'native_subagent': False,
            'caller_approvals': False, 'qualification': 'measured seed only; unmeasured versions refused',
            'configuration': 'no legacy sandbox or permission profile selectors, project configuration, managed configuration, hooks, MCP or plugins; runtime/configuration namespaces outside report root',
            'boundary': 'native OS filesystem profile; repository remains read-only; own group directory only'}


def qualify(backend):
    from .qualification import require
    result = require(backend, FEATURE)
    if result.identity.version != VERSION or result.identity.platform != 'darwin':
        raise UnsupportedCapability('group report output requires measured macOS codex-cli ' + VERSION)
    return result.as_dict()


def settings(path):
    # One whole TOML table: -c dotted keys preserve literal quotes in path components.
    return (f'default_permissions="{PROFILE}"',
            'permissions={' + PROFILE + '={extends=":read-only",filesystem={":root"="read",' +
            json.dumps(str(path)) + '="write"},network={enabled=false}}}')


def directory(path):
    from ..canonical_paths import canonical
    path = Path(path)
    if not path.is_absolute() or path.is_symlink() or not path.is_dir():
        raise ValueError('group report output requires an existing absolute real group directory')
    resolved = canonical(path)
    if str(path) != str(resolved):
        raise ValueError('group report directory must have canonical spelling and no aliases')
    info = resolved.stat()
    return resolved, (str(resolved), info.st_dev, info.st_ino, stat.S_IFMT(info.st_mode))


def runtime(backend):
    selected = shutil.which(backend.binary)
    if selected is None or not Path(selected).is_absolute():
        raise ValueError('group report output requires an absolute installed Codex launcher')
    selected = Path(selected).absolute()
    resolved = selected.resolve(strict=True)
    link, target = selected.lstat(), resolved.stat()
    return (str(selected), str(resolved), link.st_dev, link.st_ino, link.st_mtime_ns,
            link.st_size, target.st_dev, target.st_ino, target.st_mtime_ns, target.st_size)


def context(backend, invocation, path, *, prospective=False):
    from ..canonical_paths import canonical
    from .codex_config import qualify_configuration
    repo = canonical(Path(invocation.argv[invocation.argv.index('-C') + 1]))
    if path == repo or path.is_relative_to(repo) or repo.is_relative_to(path):
        raise ValueError('group report output must not overlap the repository')
    grants = (SimpleNamespace(path=str(path), kind='directory'),)
    before = runtime(backend)
    backend._assert_launcher_outside_writes(invocation, grants, before, prospective=prospective, readonly_repo=True)
    backend._assert_execution_configuration(invocation, grants)
    config = qualify_configuration(repo, [(path, 'directory')], [(path, 'directory')], group_reports=True)
    return before, config


def shape(invocation, freedom, network):
    if freedom != 'read_only' or network not in (None, False):
        raise UnsupportedCapability('group report output requires read_only with network disabled')
    if (invocation.native_subagent or invocation.caller_approvals or invocation.caller_write_approvals
            or invocation.control_pipe or invocation.control_context is not None
            or invocation.extra_writable_paths or invocation.extra_path_runtime is not None
            or invocation.extra_path_configuration is not None or invocation.allowed_commands
            or invocation.scratch_directory or invocation.shared_scratch_directories):
        raise UnsupportedCapability('group report output requires a headless read-only task without approvals, native children or additional grants')


def prospective_directory(path):
    from ..canonical_paths import canonical_component_path
    path = Path(path)
    if not path.is_absolute():
        raise ValueError('group report preview requires an absolute path')
    current = Path(path.anchor)
    for part in path.parts[1:]:
        if part in ('.', '..'):
            raise ValueError('group report preview refuses traversal')
        current /= part
        if current.is_symlink():
            raise ValueError('group report preview refuses symlink ancestors')
        actual = canonical_component_path(current)
        if actual is not None and actual != current:
            raise ValueError('group report preview requires canonical path spelling')
        if current.exists() and not current.is_dir():
            raise ValueError('group report preview requires directory ancestors')
    return path


def preview(backend, invocation, path, freedom, network=None):
    shape(invocation, freedom, network)
    if any(getattr(invocation, field) is not None for field in FIELDS):
        raise ValueError('preview requires an invocation without group report authority')
    backend.assert_safe(invocation, freedom, network)
    path = prospective_directory(path)
    qualify(backend)
    before, _ = context(backend, invocation, path, prospective=True)
    if before != runtime(backend):
        raise ValueError('Codex launcher changed during group report preview')


def configure(backend, invocation, path, freedom, network=None):
    shape(invocation, freedom, network)
    if any(getattr(invocation, field) is not None for field in FIELDS):
        raise ValueError('invocation already carries group report authority')
    backend.assert_safe(invocation, freedom, network)
    path, identity = directory(path)
    receipt = qualify(backend)
    before, config = context(backend, invocation, path)
    if before != runtime(backend):
        raise ValueError('Codex launcher changed during group report qualification')
    argv = list(invocation.argv)
    sep = argv.index('--')
    options = argv[:sep]
    at = options.index('-s')
    if options[at:at + 2] != ['-s', 'read-only']:
        raise ValueError('group report output requires the canonical read-only sandbox')
    del argv[at:at + 2]
    at = argv.index('--') - (1 if argv[argv.index('--') - 1] == 'resume' else 0)
    additions = [part for setting in settings(path) for part in ('-c', setting)]
    argv[at:at] = additions
    argv[0] = before[0]
    return replace(invocation, argv=argv, group_report_directory=str(path),
                   group_report_identity=identity, group_report_runtime=before,
                   group_report_configuration=config, group_report_qualification=receipt)


def assert_grant(backend, invocation, path, freedom, network=None):
    shape(invocation, freedom, network)
    path, identity = directory(path)
    if str(path) != invocation.group_report_directory or identity != invocation.group_report_identity:
        raise ValueError('group report directory identity changed or differs from authorized group')
    receipt = invocation.group_report_qualification
    if not isinstance(receipt, dict) or receipt.get('feature') != FEATURE or receipt.get('ok') is not True:
        raise ValueError('group report output requires a qualification receipt')
    # Requiring again validates installed binary/version/platform and any runtime invalidation.
    if qualify(backend) != receipt:
        raise ValueError('group report qualification changed before launch')
    before, config = context(backend, invocation, path)
    if before != invocation.group_report_runtime or config != invocation.group_report_configuration:
        raise ValueError('group report execution configuration or runtime changed before launch')
    if invocation.argv[0] != before[0]:
        raise ValueError('group report executable differs from qualified launcher')
    argv = list(invocation.argv)
    sep = argv.index('--')
    at = sep - (1 if argv[sep - 1] == 'resume' else 0)
    additions = [part for setting in settings(path) for part in ('-c', setting)]
    if argv[at - len(additions):at] != additions:
        raise ValueError('group report permission profile differs from the exact authorized scope')
    del argv[at - len(additions):at]
    # Restore the legacy baseline only for its existing strict argv validator. This
    # temporary object never launches; any duplicate selector/extra token is refused.
    argv[0] = backend.binary
    argv[2:2] = ['-s', 'read-only']
    baseline = replace(invocation, argv=argv, **{field: None for field in FIELDS})
    backend.assert_safe(baseline, freedom, network)
    # Configuration/namespace scans and baseline validation can take time. Reobserve
    # them after the later checks, then close the scan window for launcher and
    # directory identities rather than comparing only context's initial snapshot.
    _, final_config = context(backend, invocation, path)
    if final_config != invocation.group_report_configuration:
        raise ValueError('group report configuration changed during final validation')
    if runtime(backend) != invocation.group_report_runtime:
        raise ValueError('group report runtime changed during final validation')
    if directory(path)[1] != invocation.group_report_identity:
        raise ValueError('group report directory identity changed during final validation')


def enforcement(value, path):
    if value.freedom != 'read_only':
        raise UnsupportedCapability('group report output requires read_only')
    return replace(value, group_report_output=True, writable_roots=(str(path),),
                   mechanism='Codex native permission profile extends :read-only; one group report write root; network disabled',
                   caveats=(*(c for c in value.caveats if c != 'the sandbox rejects writes outright'), 'Repository files remain read-only; only the authorized group report directory permits agent writes.', 'Report-directory grants are checked before launch; concurrent external changes are not atomically pinned. Files in the group are shared; use task-identifiable report filenames. Group output is not trusted for future execution or configuration use.'))
