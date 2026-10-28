"""Bounded loopback-only scripted providers used by production permission qualification.

These deterministic responses execute no model calls. Never serve this helper beyond loopback.
"""
from __future__ import annotations

from contextlib import contextmanager
import http.server
import json
import threading
from typing import Callable

MAX_BODY = 4 * 1024 * 1024
MAX_REQUESTS = 64


def _anthropic(blocks, stop_reason, number):
    yield 'message_start', {'type': 'message_start', 'message': {'id': f'msg_{number}', 'type': 'message', 'role': 'assistant', 'content': [], 'model': 'claude-sonnet-4-6', 'stop_reason': None, 'stop_sequence': None, 'usage': {'input_tokens': 1, 'output_tokens': 0}}}
    for index, block in enumerate(blocks):
        initial = dict(block)
        if block['type'] == 'text':
            initial['text'] = ''
            delta = {'type': 'text_delta', 'text': block['text']}
        else:
            initial['input'] = {}
            delta = {'type': 'input_json_delta', 'partial_json': json.dumps(block['input'])}
        yield 'content_block_start', {'type': 'content_block_start', 'index': index, 'content_block': initial}
        yield 'content_block_delta', {'type': 'content_block_delta', 'index': index, 'delta': delta}
        yield 'content_block_stop', {'type': 'content_block_stop', 'index': index}
    yield 'message_delta', {'type': 'message_delta', 'delta': {'stop_reason': stop_reason, 'stop_sequence': None}, 'usage': {'output_tokens': 1}}
    yield 'message_stop', {'type': 'message_stop'}


def final_message(text='qualification complete'):
    return {'type': 'message', 'role': 'assistant', 'status': 'completed', 'content': [{'type': 'output_text', 'text': text, 'annotations': []}]}


@contextmanager
def serve(kind: str, decide: Callable):
    """Yield (loopback URL, bounded requests, handler failures)."""
    requests, errors = [], []
    lock = threading.Lock()

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            try:
                size = int(self.headers.get('Content-Length', '0'))
                if not 0 < size <= MAX_BODY or self.headers.get('Transfer-Encoding'):
                    self.send_error(413)
                    return
                if kind == 'responses' and not self.path.endswith('/responses'):
                    self.send_error(404)
                    return
                body = json.loads(self.rfile.read(size))
                if not isinstance(body, dict):
                    raise ValueError('provider request is not an object')
                with lock:
                    if len(requests) >= MAX_REQUESTS:
                        self.send_error(429)
                        return
                    requests.append(body)
                    number = len(requests)
                    answer = decide(body, number)
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.end_headers()
                if kind == 'anthropic':
                    events = _anthropic(*answer, number)
                else:
                    item = dict(answer)
                    item.setdefault('id', f'item_{number}')
                    response = {'id': f'response_{number}', 'object': 'response', 'status': 'completed', 'output': [item], 'usage': {'input_tokens': 1, 'output_tokens': 1, 'total_tokens': 2}}
                    events = [(None, {'type': 'response.created', 'response': dict(response, status='in_progress', output=[])}), (None, {'type': 'response.output_item.added', 'output_index': 0, 'item': dict(item, status='in_progress')}), (None, {'type': 'response.output_item.done', 'output_index': 0, 'item': item}), (None, {'type': 'response.completed', 'response': response})]
                for name, event in events:
                    prefix = f'event: {name}\n' if name else ''
                    self.wfile.write((prefix + 'data: ' + json.dumps(event) + '\n\n').encode())
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as exc:
                errors.append(type(exc).__name__ + ': ' + str(exc)[:200])
                self.close_connection = True

    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}', requests, errors
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
