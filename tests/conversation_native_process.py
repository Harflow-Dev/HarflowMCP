"""Local deterministic ACP-shaped peer over stdio; no network, tools or sleeps."""
import json
import sys


def send(frame):
    print(json.dumps(frame), flush=True)


for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    if method == 'initialize':
        result = {'protocolVersion': 1}
    elif method == 'session/new':
        result = {'sessionId': 'fixture-session', 'modes': {'currentModeId': 'agent', 'availableModes': [{'id': 'agent'}]}}
    elif method == 'session/prompt':
        text = request['params']['prompt'][0]['text']
        send({'method': 'session/update', 'params': {'sessionId': 'fixture-session',
              'update': {'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': text}}}})
        result = {'stopReason': 'end_turn'}
    elif method == 'session/cancel':
        continue
    else:
        raise AssertionError(method)
    send({'id': request['id'], 'result': result})
