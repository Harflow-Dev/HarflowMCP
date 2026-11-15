import shutil
import tempfile
from pathlib import Path

import pytest

import fakekit


@pytest.fixture(scope='session')
def base_install():
    """One installed fake for the whole session; the first launcher execution is warmed here with the version probe."""
    root = Path(tempfile.mkdtemp(prefix='cfk'))
    installed = fakekit.Installed.install(root)
    done = installed.run_raw(['--version'], cwd=root, timeout=300)
    assert done[0] == 0 and done[1] == (fakekit.BUNDLE / 'version.txt').read_bytes()
    yield installed
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture
def fake(base_install, tmp_path):
    """The shared install, reconfigured for this test: ledger, control and workspace folders under `tmp_path`."""
    work = tmp_path / 'work'
    cwd = tmp_path / 'cwd'
    for folder in (work, cwd):
        folder.mkdir()
    fake = base_install
    fake.work, fake.cwd, fake.real_cwd = work, cwd, str(cwd.resolve())
    fake.ledger_dir, fake.control_dir = tmp_path / 'ledger', tmp_path / 'control'
    fake.control_dir.mkdir()
    fake.stop_file = tmp_path / 'stop'
    fake.set(lifetime=90)
    yield fake
    fake.cleanup()
    fake.procs.clear()
    base_install.configure()

