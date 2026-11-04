"""Register actual OS participants before loading the immutable archived engine."""
import os
from pathlib import Path
import runpy
import sys


def main():
    folder = Path(__file__).resolve().parent
    sys.path.insert(0, str(folder))
    from client import register, install_fork_hook
    from rendezvous import ParentRendezvous, acknowledge_owner
    module_args = sys.argv[1:]
    supervisor = module_args[:2] == ['-m', 'harflow_mcp.task_supervisor']
    owner = module_args[:3] == ['-m', 'harflow_mcp.workflows', 'supervise']
    if module_args[:1] == ['-m'] and not (supervisor or owner):
        raise ValueError('unsupported S09 module invocation')
    handle = register(os.environ['S08_OBSERVER_SOCKET'], os.environ['S08_OBSERVER_NONCE'],
        'workflow-owner' if owner else 'supervisor' if supervisor else 'cli',
        control=supervisor or owner, source_profile='s09')
    rendezvous = None
    if owner:
        if len(module_args) != 6 or module_args[4] != '--root':
            raise ValueError('noncanonical workflow owner argv')
        acknowledge_owner(handle, module_args[3], module_args[5])  # Observer already armed exact parent/exit watch.
    elif not supervisor:
        install_fork_hook(handle)
        if module_args[:1] in (['workflow-start'], ['workflow-resume'], ['workflow-recover']):
            rendezvous = ParentRendezvous()
            sys.addaudithook(rendezvous.audit)
    try:
        sys.path.insert(0, os.environ['S08_ARCHIVE_SRC'])
        from backend import FixtureBackend
        from harflow_mcp import backends
        backends.BACKENDS.clear()
        backend = FixtureBackend(os.environ['S08_REAL_PYTHON'], folder / 'fake_agent.py')
        backends.BACKENDS[backend.name] = backend
        backends.DEFAULT_BACKEND = backend.name
        sys.executable = os.environ['S08_PYTHON_SHIM']
        if supervisor or owner:
            module = 'harflow_mcp.workflows' if owner else 'harflow_mcp.task_supervisor'
            sys.argv = [module, *module_args[2:]]
            runpy.run_module(module, run_name='__main__')
        else:
            from harflow_mcp.ctl import main as ctl_main
            sys.argv = ['harflow-ctl', *module_args]
            try:
                try:
                    result = ctl_main()
                except SystemExit as exc:
                    result = exc.code
            finally:
                if rendezvous:
                    rendezvous.wait()
            return result
    finally:
        if rendezvous:
            rendezvous.close()


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as exc:
        print('S09 bootstrap refused: ' + type(exc).__name__ + ': ' + str(exc), file=sys.stderr)
        raise SystemExit(126)
