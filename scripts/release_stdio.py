#!/usr/bin/env python3
"""Check a release server using sequential SDK lifecycle and bounded tree cleanup."""
import json
import os
import sys

import anyio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

REQUIRED_TOOLS = frozenset({'list_backends', 'start_task', 'get_task_status',
                            'list_tasks', 'wait_for_task', 'start_workflow'})


async def check_stdio(command, env, timeout=40):
    """Keep stdin open through initialize/list_tools; SDK owns process-tree cleanup."""
    if not command or not all(isinstance(part, str) and part for part in command):
        raise ValueError('invalid smoke command')
    parameters = StdioServerParameters(command=command[0], args=command[1:], env=dict(env))
    # The SDK shields its two-second close/kill cleanup on cancellation. The
    # caller reserves five additional seconds before its outer process deadline.
    with anyio.fail_after(timeout):
        async with stdio_client(parameters) as (read, write):
            async with ClientSession(read, write) as session:
                initialized = await session.initialize()
                info = initialized.server_info
                if info.name != 'harflow':
                    raise ValueError('unexpected release server identity')
                tools = await session.list_tools()
                if not REQUIRED_TOOLS.issubset({tool.name for tool in tools.tools}):
                    raise ValueError('required release tools missing')


def main():
    if len(sys.argv) != 2:
        raise ValueError('expected one command JSON argument')
    command = json.loads(sys.argv[1])
    if not isinstance(command, list):
        raise ValueError('expected command list')
    anyio.run(check_stdio, command, dict(os.environ))


if __name__ == '__main__':
    main()
