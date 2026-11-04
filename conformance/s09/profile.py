"""S09 immutable source injection; synthetic protocols are not backend qualification."""
import json
from pathlib import Path

from conformance.oracles import ConformanceError
from conformance.runner import digest, tree_hash, framework_identity
from conformance.s08.profile import SourceProfile as S08Profile, ALLOWED as TASK_COMMANDS
from conformance.s08.observer import Observer

WORKFLOW_ACTIONS = ('validate', 'save', 'get', 'list', 'delete', 'preview', 'start',
    'status', 'detail', 'inspect', 'assigned-input', 'pause', 'resume', 'cancel',
    'recover', 'list-runs')
ALLOWED = TASK_COMMANDS | frozenset('workflow-' + action for action in WORKFLOW_ACTIONS) | {
    'resources', 'statistics-page', 'scratch', 'worktree-list', 'slot'}


def framework_hash():
    folder = Path(__file__).parent
    return digest(json.dumps({'s09': tree_hash(folder), 's08': tree_hash(folder.parent / 's08'),
        'shared': framework_identity()}, sort_keys=True).encode())


class SourceProfile(S08Profile):
    allowed_commands = ALLOWED

    def __init__(self, *args, case_spec=None, **kwargs):
        super().__init__(*args, **kwargs)
        # Canonical copy prevents a caller-owned mutable dictionary changing selectors.
        self.case_spec = json.dumps(case_spec or {}, sort_keys=True, allow_nan=False)
        if len(self.case_spec.encode()) > 16384:
            raise ValueError('S09 case configuration exceeds 16KiB')
        if not isinstance(json.loads(self.case_spec), dict):
            raise ValueError('S09 case configuration must be an object')

    def overlay_sources(self):
        result = super().overlay_sources()
        result['observer.py'] = Path(__file__).parent.parent / 's08' / 'observer.py'
        result.update({name: Path(__file__).parent / name for name in
                       ('bootstrap.py', 'fake_agent.py', 'rendezvous.py')})
        return result

    def make_observer(self):
        self.env['S09_CASE_SPEC'] = self.case_spec
        return Observer(self.output / 'observer', source_profile='s09', allow_workflows=True)

    def framework_hash(self):
        return framework_hash()

    def command(self, argv, requiredcap=(), *, required_capabilities=None):
        argv = list(argv)
        options = [token.split('=', 1)[0] for token in argv]
        if any(option != '--freedom' and option.startswith('--') and
               '--freedom'.startswith(option) for option in options):
            raise ConformanceError('S09 requires the explicit freedom option without abbreviations')
        forbidden = ('--branch', '--base', '--fetch', '--fetch-default',
                     '--fetch-required', '--worktree-base', '--worktree-branch', '--worktree-detach')
        # argparse accepts unique long-option prefixes; the harness deliberately
        # accepts only explicit safe forms rather than delegating that authority.
        if any(option != '--worktree' and option.startswith('--') and
               any(flag.startswith(option) for flag in forbidden) for option in options):
            raise ConformanceError('S09 excludes branch, fetch and worktree mutation options')
        if argv and argv[0] == 'run' and any(option.startswith('--') and '--workflow'.startswith(option) for option in options):
            raise ConformanceError('S09 workflow launches require the explicit workflow-start path')
        if '--worktree' in options:
            freedoms = []
            for index, option in enumerate(options):
                if option == '--freedom':
                    freedoms.append(argv[index].split('=', 1)[1] if '=' in argv[index] else
                                    argv[index + 1] if index + 1 < len(argv) else None)
            if not (argv[:1] == ['run'] and freedoms == ['read_only']):
                raise ConformanceError('S09 permits only the public read-only worktree refusal probe')
        if argv and argv[0] == 'slot' and argv[1:2] != ['status']:
            raise ConformanceError('S09 permits only slot status')
        if argv and argv[0] == 'scratch':
            if argv[1:2] not in (['list'], ['clean']):
                raise ConformanceError('S09 scratch subcommand is not allowed')
            if argv[1] == 'clean':
                targets = [flag for flag in ('--chain', '--group') if flag in argv]
                if len(targets) != 1:
                    raise ConformanceError('S09 scratch cleanup requires exactly one chain/group target')
        return super().command(argv, requiredcap, required_capabilities=required_capabilities)
