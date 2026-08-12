"""Real agent runs against every installed backend. These spend real tokens.

The `integration` marker alone gates nothing — `pyproject.toml` sets no `addopts` excluding it, so a
plain `uv run pytest` would happily spend money. The `skipif` below is what makes the documented
"unit: fast, no auth, no tokens" command true, and it must stay.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from polybridge import backends
from polybridge.tasks import TaskRegistry

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("PB_INTEGRATION"),
        reason="set PB_INTEGRATION=1 to spawn real agent runs (spends real tokens)",
    ),
]

# Generous enough for a cold start on any of the three, short enough that a wedged run fails the
# suite rather than hanging it.
RUN_TIMEOUT_SECONDS = 300.0

PROMPT = "Reply with exactly: ack. Do not create, modify or delete any files."


@pytest.mark.parametrize("name", sorted(backends.BACKENDS))
async def test_a_trivial_read_only_run_completes(name: str, git_repo: Path, tmp_path: Path) -> None:
    backend = backends.get(name)
    if not backends.is_installed(backend):
        pytest.skip(f"the {backend.binary} CLI is not on PATH")

    registry = TaskRegistry(log_dir=tmp_path / "streams")
    task = await registry.start(PROMPT, git_repo, backend=backend, freedom="read_only")

    try:
        await asyncio.wait_for(task.done.wait(), RUN_TIMEOUT_SECONDS)
    except (asyncio.TimeoutError, TimeoutError):
        # Never leave a live agent behind for the next test to trip over.
        await registry.cancel(task)
        pytest.fail(
            f"{name} did not finish within {RUN_TIMEOUT_SECONDS}s; "
            f"last output: {list(task.tail)[-5:]}"
        )

    assert task.status == "completed", (
        f"{name} ended {task.status} (exit {task.exit_code}); "
        f"stderr tail: {list(task.stderr_tail)[-5:]}"
    )
    assert task.acc.summary, f"{name} completed without a closing summary"
    # Every backend must disclose an id one way or another, or the conversation cannot be continued.
    assert task.session_id, f"{name} completed without disclosing a session id"

    # Asserted from the same run rather than a second one: a backend claiming to report cost must
    # produce one, and a backend that does not must report null rather than invent a number.
    # Deliberately `>= 0`, not `> 0`: the capability is that a number arrives, and a cached, free or
    # locally-served run may legitimately cost nothing.
    if backend.capabilities.reports_cost_usd:
        assert task.acc.total_cost_usd is not None and task.acc.total_cost_usd >= 0
    else:
        assert task.acc.total_cost_usd is None
