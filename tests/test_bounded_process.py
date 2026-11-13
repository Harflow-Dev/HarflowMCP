import os
import sys
import time

from harflow_mcp import bounded_process


def _run(code, *, seconds=5.0, stdout_cap=1024, stderr_cap=1024, cwd=None, env=None):
    return bounded_process.run(
        [sys.executable, "-c", code],
        env=env if env is not None else dict(os.environ),
        cwd=cwd or os.getcwd(),
        deadline=time.monotonic() + seconds,
        stdout_cap=stdout_cap,
        stderr_cap=stderr_cap,
    )


def _alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _wait_dead(pid, seconds=3.0):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if not _alive(pid):
            return True
        time.sleep(0.05)
    return not _alive(pid)


def test_captures_both_streams_and_exit_code():
    done = _run("import sys; sys.stdout.write('out'); sys.stderr.write('err'); sys.exit(3)")
    assert (done.returncode, done.stdout, done.stderr, done.cause) == (3, b"out", b"err", None)


def test_stdin_is_dev_null(tmp_path):
    done = _run("import sys; sys.stdout.write(repr(sys.stdin.read()))", cwd=str(tmp_path))
    assert done.cause is None and done.stdout == b"''"


def test_uses_given_cwd_and_environment(tmp_path):
    done = _run("import os; print(os.getcwd()); print(os.environ.get('HF_PROBE'))",
                cwd=str(tmp_path), env={"HF_PROBE": "x"})
    lines = done.stdout.decode().split()
    assert os.path.realpath(lines[0]) == os.path.realpath(str(tmp_path)) and lines[1] == "x"


def test_runs_in_its_own_session():
    done = _run("import os; print(os.getsid(0) == os.getpid())")
    assert done.stdout.strip() == b"True"


def test_deadline_kills_the_process_group_including_a_sigterm_ignoring_grandchild():
    code = (
        "import os, signal, subprocess, sys, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "child = subprocess.Popen([sys.executable, '-c', "
        "'import signal,time,os; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "print(os.getpid(), flush=True); time.sleep(60)'], stdout=sys.stdout, stderr=sys.stderr)\n"
        "time.sleep(60)\n"
    )
    start = time.monotonic()
    done = _run(code, seconds=1.0, stdout_cap=1024)
    assert time.monotonic() - start < 5
    assert done.cause == bounded_process.TIMEOUT and done.returncode is None
    grandchild = int(done.stdout.split()[0]) if done.stdout.split() else None
    if grandchild is not None:
        assert _wait_dead(grandchild)


def test_grandchild_holding_the_pipes_after_the_parent_exits_times_out():
    code = (
        "import subprocess, sys\n"
        "subprocess.Popen([sys.executable, '-c', 'import time,os; print(os.getpid(), flush=True); time.sleep(60)'])\n"
    )
    start = time.monotonic()
    done = _run(code, seconds=1.0)
    assert time.monotonic() - start < 5
    assert done.cause == bounded_process.TIMEOUT
    grandchild = int(done.stdout.split()[0])
    assert _wait_dead(grandchild)


def test_stdout_flood_hits_the_ceiling_and_keeps_only_the_cap():
    done = _run("import sys\nwhile True: sys.stdout.write('x' * 65536); sys.stdout.flush()",
                stdout_cap=1000, stderr_cap=1000)
    assert done.cause == bounded_process.OUTPUT_CEILING and done.returncode is None
    assert len(done.stdout) == 1000


def test_stderr_flood_hits_the_ceiling_with_its_own_cap():
    done = _run("import sys\nwhile True: sys.stderr.write('e' * 65536); sys.stderr.flush()",
                stdout_cap=10**6, stderr_cap=500)
    assert done.cause == bounded_process.OUTPUT_CEILING
    assert len(done.stderr) == 500 and done.stdout == b""


def test_simultaneous_floods_stop_on_the_first_ceiling():
    code = ("import sys\nwhile True:\n sys.stdout.write('o' * 65536); sys.stdout.flush()\n"
            " sys.stderr.write('e' * 65536); sys.stderr.flush()")
    start = time.monotonic()
    done = _run(code, stdout_cap=2000, stderr_cap=2000)
    assert time.monotonic() - start < 5
    assert done.cause == bounded_process.OUTPUT_CEILING
    assert len(done.stdout) <= 2000 and len(done.stderr) <= 2000


def test_output_exactly_at_the_cap_is_not_a_ceiling():
    done = _run("import sys; sys.stdout.write('x' * 100)", stdout_cap=100)
    assert done.cause is None and len(done.stdout) == 100


def test_spawn_failure_is_a_cause_not_an_exception(tmp_path):
    done = bounded_process.run([str(tmp_path / "missing")], env={}, cwd=str(tmp_path),
                               deadline=time.monotonic() + 5, stdout_cap=10, stderr_cap=10)
    assert done == bounded_process.Completed(None, b"", b"", bounded_process.SPAWN_FAILED)


def test_bad_cwd_is_spawn_failed(tmp_path):
    done = bounded_process.run([sys.executable, "-c", "pass"], env=dict(os.environ), cwd=str(tmp_path / "nope"),
                               deadline=time.monotonic() + 5, stdout_cap=10, stderr_cap=10)
    assert done.cause == bounded_process.SPAWN_FAILED


def test_expired_deadline_spawns_nothing(tmp_path):
    marker = tmp_path / "ran"
    done = bounded_process.run(
        [sys.executable, "-c", f"open({str(marker)!r}, 'w').close()"], env=dict(os.environ), cwd=str(tmp_path),
        deadline=time.monotonic() - 1, stdout_cap=10, stderr_cap=10)
    assert done.cause == bounded_process.TIMEOUT and not marker.exists()
