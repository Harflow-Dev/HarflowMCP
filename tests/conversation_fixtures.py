"""Deterministic provider-only doubles for real conversation registry tests."""
from __future__ import annotations

import asyncio
import json
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from harflow_mcp.backends import Invocation
from harflow_mcp.backends.base import Enforcement
from harflow_mcp.backends.claude import ClaudeBackend


@dataclass
class ProviderTurn:
    request: dict
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    async def result(self, text: str) -> None:
        self.writer.write((json.dumps({"result": text}) + "\n").encode())
        await self.writer.drain()
        assert await asyncio.wait_for(self.reader.readline(), 5) == b"result_written\n"

    async def settle(self) -> None:
        self.writer.write(b'{"exit": true}\n')
        await self.writer.drain()
        self.writer.close()
        await self.writer.wait_closed()


class ConversationProvider(ClaudeBackend):
    """The registry, monitor and persistence stay real; only the provider is fake.

    The subprocess emits a terminal protocol result on one barrier and exits on
    another, allowing tests to distinguish response completion from settlement.
    """

    name = "conversation-fixture"
    binary = sys.executable

    def __init__(self, socket_path: Path):
        self._socket_directory = tempfile.TemporaryDirectory(prefix="hf-conv-", dir="/tmp")
        self.socket_path = Path(self._socket_directory.name) / "provider.sock"
        self.calls: asyncio.Queue[ProviderTurn] = asyncio.Queue()
        self.history: list[dict] = []
        self.server = None
        self.turns: list[ProviderTurn] = []

    async def open(self):
        self.server = await asyncio.start_unix_server(self._connect, path=str(self.socket_path))
        return self

    async def _connect(self, reader, writer):
        request = json.loads(await reader.readline())
        turn = ProviderTurn(request, reader, writer)
        self.history.append(request)
        self.turns.append(turn)
        await self.calls.put(turn)

    async def next_turn(self):
        return await asyncio.wait_for(self.calls.get(), 10)

    def _invocation(self, prompt, kwargs, resumed):
        return Invocation([
            sys.executable, str(Path(__file__).with_name("conversation_provider_process.py")),
            str(self.socket_path), json.dumps({"prompt": prompt,
                "session_id": kwargs.get("session_id"), "resumed": resumed}),
        ])

    def build_start_argv(self, prompt, **kwargs):
        return self._invocation(prompt, kwargs, False)

    def build_resume_argv(self, prompt, *, session_id, **kwargs):
        return self._invocation(prompt, {**kwargs, "session_id": session_id}, True)

    def conversation_resume_supported(self, source):
        return True

    def with_system_guidance(self, invocation, text, **kwargs):
        return invocation

    def assert_safe(self, invocation, freedom, network=None):
        assert isinstance(invocation, Invocation)
        assert invocation.argv[:2] == [sys.executable, str(Path(__file__).with_name("conversation_provider_process.py"))]
        assert invocation.argv[2] == str(self.socket_path)
        assert len(invocation.argv) == 4

    def enforcement(self, freedom, network=None):
        return Enforcement(freedom=freedom, mechanism="fixture", os_enforced=False, writes_confined=False)

    async def close(self):
        for turn in self.turns:
            if not turn.writer.is_closing():
                turn.writer.close()
                await turn.writer.wait_closed()
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        self._socket_directory.cleanup()
