"""How the lifecycle cases obtain a running fake agent: the open fake-backend seam.

`NoSeam` is the only seam that exists for a pinned engine today: it has no fake, so every case that needs a
running agent is *blocked*, never passed. `PythonInjectionSeam` re-uses the S08 controlled-source profile (an
in-process backend replacement plus a shim) so the case logic can be proven against Python 0.0.11; it is not a
pinned binary, uses extra environment, and its runs are never eligible for acceptance. The proposed real seam is
written up in docs/decisions/E2-lifecycle-conformance-seam.md and deliberately not built here.
"""
from pathlib import Path
import time

from ..oracles import ConformanceError
from .observer import ProcessWatch
from .target import ENV_ALLOWLIST


class SeamRequired(ConformanceError):
    """The case needs a running fake agent and the selected seam has none."""


def require(condition, message, tag='baseline'):
    if not condition:
        raise ConformanceError(f'{message} [{tag}]')


class NoSeam:
    name = 'none'
    acceptance_eligible = True
    fake_available = False
    description = 'pinned engine, no fake agent: only cases that never start an agent can run'

    def __init__(self, engine, workspace, markers=()):
        self.engine, self.workspace = engine, Path(workspace)
        self.markers = tuple(m for m in markers if m)
        self.operations = []
        self.evidence = {}

    @property
    def extra_env_keys(self):
        return list(self.engine.extra_env_keys) if self.engine is not None else []

    def call(self, argv, *, refused=False):
        reply = self.engine.command([*argv, '--json'])
        doc = reply['document'] or {}
        self.operations.append({'argv': list(argv), 'exit_code': reply['exit_code'], 'document': doc, 'process': reply['process']})
        require((reply['exit_code'] != 0 and isinstance(doc.get('error'), dict)) if refused
                else (reply['exit_code'] == 0 and 'error' not in doc), 'unexpected public refusal/success: ' + argv[0], 'contract')
        return doc

    def extra_refusals(self):
        return []

    def task_ids(self):
        return [row['task_id'] for row in self.call(['list'])['tasks']]

    def watch_spawns(self):
        """Independent OS evidence for a refusal: a `ps` snapshot diff for processes naming the isolated case or the
        pinned engine. It sees only processes still alive at the end, so it complements (does not replace) the public
        task listing. When the process table cannot be read, this raises and the refusal case cannot pass."""
        watch = ProcessWatch(self.markers)

        def verify():
            started = watch.new_matching()
            self.evidence['spawn_check'] = {'method': 'ps-snapshot-diff', 'markers': list(self.markers), 'new_matching': started,
                                            'limits': 'processes that exited before the final snapshot are not seen'}
            require(not started, f'a refused request left processes running: {started}', 'contract')
        return verify

    def launch(self, *args, **kwargs):
        raise SeamRequired('this case starts an agent and the selected seam provides no fake')

    resume = launch

    def close(self):
        return self.engine.clean() if self.engine is not None else True


class PythonInjectionSeam:
    """Python-only validation seam built on the S08 profile. Never acceptance evidence."""
    name = 'python-injection'
    acceptance_eligible = False
    fake_available = True
    description = 'S08 controlled-source fixture injection (shim, in-process backend replacement, extra environment)'

    def __init__(self, repo, commit, python, output):
        from ..s08.profile import SourceProfile
        from ..s08.scenarios import Case
        self.engine = None
        self.evidence = {}
        self.profile = SourceProfile(repo, commit, Path(output), python, tasks_enabled=True)
        self.profile.__enter__()
        try:
            self.case = Case(self.profile)
            self.case.init_git()
        except BaseException:
            self.close()
            raise
        self.workspace = self.profile.workspace

    @property
    def extra_env_keys(self):
        return sorted(set(self.profile.env) - ENV_ALLOWLIST)

    @property
    def operations(self):
        return self.case.operations

    def call(self, argv, *, refused=False):
        return self.case.call(list(argv), refused=refused)

    def extra_refusals(self):
        w = self.workspace
        return [['run', '--backend', 'conformance_fake', '--repo', str(w / 'missing'), '--prompt', 'must not spawn'],
                ['run', '--backend', 'conformance_fake', '--repo', str(w), '--prompt', 'must not spawn', '--max-turns', '1']]

    def task_ids(self):
        for _ in range(40):
            result = self.call(['task-list-page'])['result']
            if not result.get('bootstrap_pending'):
                return [row['task_id'] for row in result['items']]
            time.sleep(.25)
        raise ConformanceError('task listing never left preparation')

    def fakes_spawned(self):
        return len([r for r in self.profile.observer.records if r['role'] == 'fake'])

    def watch_spawns(self):
        before = self.fakes_spawned()

        def verify():
            ledgers = len(list((self.profile.output / 'fake-ledger').iterdir()))
            self.evidence['spawn_check'] = {'method': 'observer-registrations', 'fakes_before': before, 'fakes_after': self.fakes_spawned(), 'ledgers': ledgers}
            require(self.fakes_spawned() == before and ledgers == 0, 'a refused request spawned a fake or wrote fake evidence', 'contract')
        return verify

    def launch(self, mode='complete', **spec):
        item = self.case.launch(mode, **spec)
        return {**item, 'owner': item['supervisor']}

    def resume(self, launch):
        item = self.case.resume(launch)
        return {**item, 'owner': item['supervisor']}

    def exited(self, participant, timeout=5):
        return self.profile.observer.wait_exited(participant['token'], timeout)

    def alive(self, participant):
        return not next(r for r in self.profile.observer.records if r['token'] == participant['token'])['exited']

    def kill_owner(self, launch):
        return self.profile.observer.kill_participant(launch['owner']['token'])

    def raw_evidence(self, launch, completed=False):
        return self.case.raw(launch, completed=completed)

    def close(self):
        try:
            self.profile.__exit__(None, None, None)
            return True
        except ConformanceError:
            return False
