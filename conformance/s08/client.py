"""Cooperative S08 bootstrap; SAME_UID transport is not hostile-peer authentication."""
import atexit
import json
import os
import signal
import select
import socket
import threading

LIMIT = 4096


def _read(sock):
    data = bytearray()
    while len(data) <= LIMIT:
        chunk = sock.recv(1)
        if not chunk:
            raise RuntimeError('observer disconnected')
        if chunk == b'\n':
            return json.loads(data)
        data.extend(chunk)
    raise RuntimeError('observer frame exceeded')


class Handle:
    def __init__(self, sock, path, nonce, profile, token, control):
        self.socket, self.path, self.nonce = sock, path, nonce
        self.source_profile, self.token, self.control = profile, token, control
        self.normal = False
        atexit.register(self.close)
        if control:
            threading.Thread(target=self._receive, daemon=True).start()
        else:
            sock.close()

    def _receive(self):
        try:
            command = _read(self.socket)
            if command != {'command': 'self-kill', 'token': self.token}:
                raise RuntimeError('invalid observer command')
        except BaseException:
            if self.normal:
                return
        if not self.normal:
            os.kill(os.getpid(), signal.SIGKILL)

    def close(self):
        self.normal = True
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.socket.close()


def register(socket_path, nonce, role, control=True, source_profile='s08'):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(3)
    try:
        sock.connect(str(socket_path))
        request = dict(nonce=nonce, role=role, pid=os.getpid(), control=control,
                       source_profile=source_profile)
        sock.sendall(json.dumps(request).encode() + b'\n')
        reply = _read(sock)
        if reply.get('ok') is not True or not isinstance(reply.get('token'), str):
            raise RuntimeError('observer registration refused')
        sock.settimeout(None)
        return Handle(sock, str(socket_path), nonce, source_profile, reply['token'], control)
    except BaseException:
        sock.close()
        raise


def install_fork_hook(handle):
    """Call only on the thread-free control=False CLI before its stock fork."""
    if handle.control:
        raise ValueError('fork hook requires control=False registration')

    pipe = []

    def before():
        pipe[:] = os.pipe()

    def parent():
        os.close(pipe[1])
        try:
            # Keep the real CLI parent alive until its child has registered.
            if not select.select([pipe[0]], [], [], 4)[0] or os.read(pipe[0], 1) != b'1':
                os._exit(126)
        finally:
            os.close(pipe[0])

    def child():
        os.close(pipe[0])
        handle.close()
        try:
            register(handle.path, handle.nonce, 'ctl-fork', source_profile=handle.source_profile)
            os.write(pipe[1], b'1')
            os.close(pipe[1])
        except BaseException:
            os._exit(126)  # Python ignores exceptions escaping at-fork callbacks.

    os.register_at_fork(before=before, after_in_parent=parent, after_in_child=child)
