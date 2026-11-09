"""Cooperative, synchronous read/probe budget for private observers."""
from contextvars import ContextVar
from time import monotonic


class Exhausted(ValueError):
    pass


class Budget:
    def __init__(self, seconds=5, byte_limit=8 * 1024 * 1024):
        self.deadline = monotonic() + seconds
        self.metadata_limit = byte_limit
        self.metadata_bytes = 0
        self.exhausted = False

    def remaining(self):
        remaining = self.deadline - monotonic()
        if self.exhausted or remaining <= 0:
            self.exhausted = True
            raise Exhausted('Observation deadline reached')
        return remaining

    def charge(self, size):
        self.remaining()
        if size > self.metadata_limit - self.metadata_bytes:
            self.exhausted = True
            raise Exhausted('Observation read allowance reached')
        self.metadata_bytes += size


current = ContextVar('private_observation_budget', default=None)
