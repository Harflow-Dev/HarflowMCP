"""Client side of the ancestry rendezvous, for the fake participant (standard library only)."""
import json
import os
import signal
import socket


def register(socket_path, nonce, role='fake', timeout=5):
    """Register with the observer and block until it has armed its exit watches. Returns the acknowledgement."""
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(timeout)
        conn.connect(socket_path)
        conn.sendall(json.dumps({'nonce': nonce, 'role': role, 'pid': os.getpid()}).encode() + b'\n')
        data = b''
        while not data.endswith(b'\n'):
            chunk = conn.recv(512)
            if not chunk:
                raise RuntimeError('observer closed before acknowledging')
            data += chunk
    ack = json.loads(data)
    if not ack.get('ok'):
        raise RuntimeError('observer refused registration')
    return ack


def kill_parent(ack):
    """End the process that spawned this participant, if it is still the one the observer recorded.

    The participant is a child of the engine's owner, so this is the participant's own parent handle, not a
    saved PID of the harness: if the parent changed (it died and the participant was reparented) nothing is
    signalled. Returns whether a signal was sent."""
    parent = os.getppid()
    if parent != ack['ppid'] or parent in (0, 1):
        return False
    os.kill(parent, signal.SIGKILL)
    return True


def bound_lifetime(seconds):
    """Hard lifetime bound: the participant exits by itself after `seconds` whatever else happens, so a failed
    harness can never leave it running. (A fake should also exit when its engine pipes reach EOF.)"""
    def expire(*_):
        os._exit(70)
    signal.signal(signal.SIGALRM, expire)
    signal.alarm(int(seconds))
