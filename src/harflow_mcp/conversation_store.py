"""Per-conversation leaf ledger. No effect, subprocess or await under its lock."""
from __future__ import annotations
import errno
import fcntl
import json
import os
from pathlib import Path
import stat
import time
import uuid
from .conversation_documents import ConversationError, MAX_DOCUMENT_BYTES, encoded, validate, request_uuid
from .state_ownership import assert_owned

class ConversationStore:
    def __init__(self, layout, conversation_id, *, lock_timeout=2.0):
        self.layout = layout
        self.conversation_id = request_uuid(conversation_id)
        self.directory = layout.root / 'conversations'
        self.path = self.directory / (conversation_id + '.json')
        self.lock_path = self.directory / (conversation_id + '.lock')
        self.lock_timeout = lock_timeout

    def _read(self):
        try:
            fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
        except FileNotFoundError:
            return None
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_DOCUMENT_BYTES:
                raise ConversationError('ledger_unavailable')
            with os.fdopen(fd, 'rb', closefd=False) as stream:
                raw = stream.read(MAX_DOCUMENT_BYTES + 1)
            if len(raw) > MAX_DOCUMENT_BYTES: raise ConversationError('ledger_unavailable')
            def pairs(items):
                result = {}
                for key,value in items:
                    if key in result: raise ConversationError('invalid_document')
                    result[key] = value
                return result
            document = validate(json.loads(raw.decode('utf-8'), object_pairs_hook=pairs))
            if document['conversation_id'] != self.conversation_id: raise ConversationError('identity_mismatch')
            return document
        except (ValueError,UnicodeError,RecursionError) as exc:
            raise ConversationError('ledger_unavailable') from exc
        finally:
            os.close(fd)

    def _write(self, document):
        payload = encoded(validate(document))
        pending = self.directory / ('.' + self.conversation_id + '.' + str(uuid.uuid4()) + '.pending')
        fd = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        try:
            with os.fdopen(fd, 'wb', closefd=False) as stream:
                stream.write(payload); stream.flush(); os.fsync(fd)
            os.replace(pending,self.path)
            directory_fd = os.open(self.directory,os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            try: os.fsync(directory_fd)
            finally: os.close(directory_fd)
        finally:
            os.close(fd)
            try: pending.unlink()
            except FileNotFoundError: pass

    def update(self, mutation):
        """mutation(old|None) -> (new_document, result). Exceptions publish nothing.

        A publication error is uncertain: reopen under this same lock before deciding
        whether a claim exists. Never issue an effect on the failed return path.
        """
        assert_owned(self.layout)
        self.directory.mkdir(mode=0o700,exist_ok=True)
        if not stat.S_ISDIR(self.directory.lstat().st_mode): raise ConversationError('ledger_unavailable')
        fd = os.open(self.lock_path,os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,0o600)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode): raise ConversationError('ledger_unavailable')
            deadline = time.monotonic() + self.lock_timeout
            while True:
                try:
                    fcntl.flock(fd,fcntl.LOCK_EX | fcntl.LOCK_NB); break
                except OSError as exc:
                    if exc.errno not in (errno.EACCES,errno.EAGAIN): raise
                    if time.monotonic() >= deadline: raise ConversationError('ledger_busy')
                    time.sleep(.005)
            lock_info = self.lock_path.lstat()
            held_info = os.fstat(fd)
            if (lock_info.st_dev, lock_info.st_ino) != (held_info.st_dev, held_info.st_ino):
                raise ConversationError('ledger_unavailable')
            old = self._read()
            document,result = mutation(old)
            validate(document)
            if document['conversation_id'] != self.conversation_id: raise ConversationError('identity_mismatch')
            if document != old: self._write(document)
            return result
        finally:
            os.close(fd)

    def read(self):
        def read_only(document):
            if document is None: raise ConversationError('conversation_missing')
            return document,document
        return self.update(read_only)
