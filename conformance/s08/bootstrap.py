"""Isolated entry point registering participants before loading the archived engine."""
import os
from pathlib import Path
import runpy
import sys

def main():
    folder = Path(__file__).resolve().parent
    sys.path.insert(0, str(folder))
    from client import register, install_fork_hook
    module_args = sys.argv[1:]
    supervisor = module_args[:2] == ['-m', 'harflow_mcp.task_supervisor']
    handle = register(os.environ['S08_OBSERVER_SOCKET'], os.environ['S08_OBSERVER_NONCE'],
                      'supervisor' if supervisor else 'cli', control=supervisor)
    if not supervisor:
        install_fork_hook(handle)
    sys.path.insert(0, os.environ['S08_ARCHIVE_SRC'])
    from backend import FixtureBackend
    from harflow_mcp import backends
    backends.BACKENDS.clear()
    backend = FixtureBackend(os.environ['S08_REAL_PYTHON'], folder / 'fake_agent.py')
    backends.BACKENDS[backend.name] = backend
    backends.DEFAULT_BACKEND = backend.name
    sys.executable = os.environ['S08_PYTHON_SHIM']
    if supervisor:
        sys.argv = ['harflow_mcp.task_supervisor', *module_args[2:]]
        runpy.run_module('harflow_mcp.task_supervisor', run_name='__main__')
    else:
        from harflow_mcp.ctl import main as ctl_main
        sys.argv = ['harflow-ctl', *module_args]
        raise SystemExit(ctl_main())

if __name__ == '__main__':
    main()
