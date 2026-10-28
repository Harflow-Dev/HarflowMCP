"""Lazy, feature-specific qualification receipts bound to an installed binary.

Seed rows trust a maintainer's version/platform measurement, not binary provenance. Auto probes
must provide their own OS isolation; a localhost provider override alone is insufficient.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from contextlib import contextmanager
import fcntl
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import shutil
import signal
import stat
import sys
import tempfile
import time
import uuid
from typing import Any, Callable, Mapping

from .base import UnsupportedCapability
from ..wire_contracts import PRODUCT, PRODUCT_VERSION
from ..state_compatibility import persisted_compatible

MAX_RECEIPT_BYTES = 65536


@dataclass(frozen=True)
class Identity:
    backend: str
    version: str
    executable: str
    binary_sha256: str
    platform: str


@dataclass(frozen=True)
class ProbeContext:
    identity: Identity
    feature: str
    scratch: str


@dataclass(frozen=True)
class ProbeResult:
    ok: bool
    reason: str
    facts: dict[str, Any] = field(default_factory=dict)
    # Successful sibling features measured by the same isolated probe bundle.
    features: dict[str, dict[str, Any]] = field(default_factory=dict)
    failures: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class FeatureSpec:
    feature: str
    revision: str
    seeds: Mapping[str, dict[str, Any]] = field(default_factory=dict)
    probe: Callable[[ProbeContext], ProbeResult] | None = None
    deny_versions: Mapping[str, str] = field(default_factory=dict)
    timeout_seconds: float = 60.0
    platforms: tuple[str, ...] = ()
    seed_platforms: tuple[str, ...] = ()


@dataclass(frozen=True)
class QualificationResult:
    identity: Identity
    feature: str
    revision: str
    ok: bool
    source: str
    reason: str
    facts: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


# Receipts used by synchronous protocol adapters; never populate from an unqualified version string.
_receipts: dict[tuple[str, str], dict[str, Any]] = {}
_identities: dict[tuple[str, str], Identity] = {}
_bundle_facts: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}


def runtime_facts(backend: str, version: str) -> dict[str, Any] | None:
    return _receipts.get((backend, version))


def runtime_identity(backend_name: str, version: str) -> Identity | None:
    return _identities.get((backend_name, version))


def identity_for(backend: Any, version: str) -> Identity | None:
    """Last successful receipt in this process; capture at launch for later invalidation."""
    return _identities.get((backend.name, version))


def identify(backend: Any) -> Identity:
    from . import version
    selected = shutil.which(backend.binary)
    if not selected:
        raise UnsupportedCapability(f"qualification: {backend.binary} is not installed")
    path = Path(selected).resolve(strict=True)
    before = path.stat()
    if not stat.S_ISREG(before.st_mode):
        raise UnsupportedCapability("qualification binary is not a regular file")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    observed = backend.qualification_version(version(backend))
    after = path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise UnsupportedCapability("qualification binary changed while hashing")
    if not observed:
        raise UnsupportedCapability("qualification could not determine the installed version")
    return Identity(backend.name, observed, str(path), digest.hexdigest(), sys.platform)


def _key(identity: Identity, revision: str) -> str:
    if len(identity.binary_sha256) != 64 or any(c not in "0123456789abcdef" for c in identity.binary_sha256):
        raise UnsupportedCapability("qualification binary hash is invalid")
    return hashlib.sha256(json.dumps([asdict(identity), revision], sort_keys=True).encode()).hexdigest()


def _layout(root):
    from ..state_layout import resolve_layout
    from ..state_ownership import current_owned, StateOwnershipError
    if root is None:
        try:
            return current_owned().layout
        except StateOwnershipError:
            pass
    return resolve_layout(root=root)


def _read(path: Path, identity: Identity, revision: str, directory_fd=None) -> dict:
    try:
        fd = os.open(path if directory_fd is None else path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
    except FileNotFoundError:
        return {}
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("not regular")
        raw = os.read(fd, MAX_RECEIPT_BYTES + 1)
    finally:
        os.close(fd)
    value = json.loads(raw)
    if (len(raw) > MAX_RECEIPT_BYTES or not isinstance(value, dict) or type(value.get("v")) is not int or value.get("v") != 1
            or value.get("product") != PRODUCT or value.get("format") != "harflow.qualification"
            or not persisted_compatible(value.get("product_version"), "harflow.qualification", value.get("v"))
            or value.get("identity") != asdict(identity) or value.get("revision") != revision
            or not isinstance(value.get("features"), dict) or len(value["features"]) > 64):
        raise ValueError("invalid qualification receipt")
    for name, row in value["features"].items():
        if (not isinstance(name, str) or len(name) > 128 or not isinstance(row, dict) or type(row.get("ok")) is not bool
                or row.get("source") not in ("seeded", "auto-probed", "failed", "failed-at-runtime")
                or not isinstance(row.get("reason"), str) or len(row["reason"]) > 1000 or not isinstance(row.get("facts"), dict)
                or (row.get("ok") and not row["facts"].get(name))):
            raise ValueError("invalid feature receipt")
    return value


def _write(path: Path, value: dict, directory_fd: int) -> None:
    raw = json.dumps(value, sort_keys=True, allow_nan=False).encode()
    if len(raw) > MAX_RECEIPT_BYTES:
        raise ValueError("qualification receipt exceeds budget")
    temporary = ".qualification-" + uuid.uuid4().hex
    fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600, dir_fd=directory_fd)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path.name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        os.fsync(directory_fd)
    finally:
        try:
            os.unlink(temporary, dir_fd=directory_fd)
        except FileNotFoundError:
            pass


@contextmanager
def _existing_read_scope(layout):
    """Read under the existing ownership lock without initialization or source promotion."""
    from ..state_ownership import current_owned, StateOwnershipError, LOCK_NAME, _marker
    try:
        lease = current_owned()
    except StateOwnershipError:
        lease = None
    if lease is not None and lease.layout == layout:
        lease.validate()
        yield
        lease.validate()
        return
    fd = os.open(layout.root / LOCK_NAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("state ownership lock is not regular")
        deadline = time.monotonic() + 2
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise ValueError("state ownership read lock timed out")
                time.sleep(.025)
        if not _marker(layout.root):
            raise ValueError("state ownership marker is missing")
        if (os.fstat(fd).st_dev, os.fstat(fd).st_ino) != (os.lstat(layout.root / LOCK_NAME).st_dev, os.lstat(layout.root / LOCK_NAME).st_ino):
            raise ValueError("state ownership lock changed")
        yield
    finally:
        os.close(fd)


def _child(connection, callback, context):
    try:
        os.setsid()
        result = callback(context)
        if not isinstance(result, ProbeResult):
            raise ValueError("probe did not return ProbeResult")
        raw = json.dumps(asdict(result), allow_nan=False).encode()
        if len(raw) > MAX_RECEIPT_BYTES:
            raise ValueError("probe output exceeds budget")
        connection.send_bytes(raw)
    except BaseException as exc:
        connection.send_bytes(json.dumps(asdict(ProbeResult(False, f"probe failed: {type(exc).__name__}: {str(exc)[:500]}"))).encode())
    finally:
        connection.close()


def _probe(spec: FeatureSpec, identity: Identity) -> ProbeResult:
    if spec.probe is None:
        return ProbeResult(False, "no isolated no-paid probe is available for this feature")
    if not 0 < spec.timeout_seconds <= 300:
        return ProbeResult(False, "invalid probe time limit")
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    with tempfile.TemporaryDirectory(prefix="harflow-qualification-") as scratch:
        process = context.Process(target=_child, args=(child, spec.probe, ProbeContext(identity, spec.feature, scratch)))
        try:
            process.start()
            child.close()
            if not parent.poll(spec.timeout_seconds):
                return ProbeResult(False, "probe timed out")
            value = json.loads(parent.recv_bytes(MAX_RECEIPT_BYTES))
            if (type(value.get("ok")) is not bool or not isinstance(value.get("reason"), str)
                    or not isinstance(value.get("facts"), dict) or not isinstance(value.get("features"), dict)
                    or not isinstance(value.get("failures"), dict)
                    or any(not isinstance(k, str) or not isinstance(v, str) for k, v in value["failures"].items())
                    or any(not isinstance(k, str) or not isinstance(v, dict) for k, v in value["features"].items())):
                return ProbeResult(False, "malformed probe result")
            return ProbeResult(**value)
        except Exception as exc:
            return ProbeResult(False, f"probe unavailable: {type(exc).__name__}: {str(exc)[:500]}")
        finally:
            parent.close()
            child.close()
            if process.pid:
                # The worker creates its own session; also stop its external harness descendants.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.join(timeout=2)
                if process.is_alive():
                    process.kill()
                    process.join(timeout=2)


def _feature_facts(name, facts, specs):
    """A feature payload cannot silently authorize another declared feature."""
    return {key: value for key, value in facts.items() if key == name or key not in specs}


def _result(identity, spec, row, rows=None, specs=None):
    result = QualificationResult(identity, spec.feature, spec.revision, row["ok"], row["source"], row["reason"], row["facts"])
    key = (identity.backend, identity.version)
    if rows is not None:
        if _identities.get(key) != identity:
            _bundle_facts.pop(key, None)
        bundle = {}
        for name, entry in rows.items():
            if entry["ok"] and name in specs:
                bundle.update(_feature_facts(name, entry["facts"], specs))
        _bundle_facts.setdefault(key, {})[spec.revision] = bundle
        merged = {name: value for facts in _bundle_facts[key].values() for name, value in facts.items()}
        if merged:
            _receipts[key] = merged
        else:
            _receipts.pop(key, None)
        _identities[key] = identity
    elif result.ok:
        if _identities.get(key) != identity:
            _receipts.pop(key, None)
            _bundle_facts.pop(key, None)
        _identities[key] = identity
        _receipts.setdefault(key, {}).update(result.facts)
    return result


def _operate(backend, feature, *, root=None, refresh=False, failure=None, read_only=False, identity=None):
    from ..state_ownership import state_session
    specs = backend.qualification_features()
    spec = specs.get(feature)
    if spec is None:
        raise UnsupportedCapability(f"{backend.name} does not declare qualification feature {feature}")
    identity = identity or identify(backend)
    key = _key(identity, spec.revision)
    if not read_only and failure is None and _identities.get((identity.backend, identity.version)) != identity:
        _receipts.pop((identity.backend, identity.version), None)
    layout = _layout(root)
    def unmeasured():
        if identity.version in spec.deny_versions:
            return QualificationResult(identity, feature, spec.revision, False, "failed", spec.deny_versions[identity.version])
        if (spec.seeds.get(identity.version, {}).get(feature) and (not spec.seed_platforms or identity.platform in spec.seed_platforms)
                and (not spec.platforms or identity.platform in spec.platforms)):
            return QualificationResult(identity, feature, spec.revision, True, "seeded",
                                       "trusted maintainer version/platform seed; binary provenance was not measured", _feature_facts(feature, spec.seeds[identity.version], specs))
        if spec.probe is None:
            return QualificationResult(identity, feature, spec.revision, False, "unsupported", "no isolated no-paid probe is available for this feature")
        return QualificationResult(identity, feature, spec.revision, False, "unknown", "qualification has not run")
    if read_only:
        # Inspect only an already initialized store. Missing infrastructure is unknown, not a write.
        from ..state_ownership import MARKER_NAME, LOCK_NAME
        if not (layout.root / MARKER_NAME).is_file() or not (layout.root / LOCK_NAME).is_file():
            return unmeasured()
        with _existing_read_scope(layout):
            directory = layout.root / "qualification"
            if directory.is_symlink():
                raise UnsupportedCapability("qualification directory is a symlink")
            if not directory.exists():
                return unmeasured()
            try:
                directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    document = _read(directory / f"{key}.json", identity, spec.revision, directory_fd)
                finally:
                    os.close(directory_fd)
            except (OSError, ValueError) as exc:
                raise UnsupportedCapability(f"qualification state unavailable: {exc}") from exc
            row = document.get("features", {}).get(feature)
            return _result(identity, spec, row, document["features"], specs) if row else unmeasured()
    try:
        with state_session(layout):
            directory = layout.root / "qualification"
            if directory.is_symlink():
                raise ValueError("qualification directory is a symlink")
            directory.mkdir(mode=0o700, exist_ok=True)
            path = directory / f"{key}.json"
            directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            lock = None
            try:
                lock = os.open(f"{key}.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600, dir_fd=directory_fd)
                if not stat.S_ISREG(os.fstat(lock).st_mode):
                    raise ValueError("qualification lock is not regular")
                deadline = time.monotonic() + 305
                while True:
                    try:
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if time.monotonic() >= deadline:
                            raise ValueError("qualification lock timed out")
                        time.sleep(.025)
                document = _read(path, identity, spec.revision, directory_fd) or {"product": PRODUCT, "format": "harflow.qualification", "product_version": PRODUCT_VERSION, "v": 1, "identity": asdict(identity), "revision": spec.revision, "features": {}}
                rows = document["features"]
                if failure is not None:
                    rows[feature] = {"ok": False, "source": "failed-at-runtime", "reason": failure[:1000], "facts": {}}
                    for sibling, other in specs.items():
                        if other.revision == spec.revision:
                            rows[sibling] = dict(rows[feature])
                    if _identities.get((identity.backend, identity.version)) == identity:
                        _receipts.pop((identity.backend, identity.version), None)
                elif feature in rows and not refresh:
                    return _result(identity, spec, rows[feature], rows, specs)
                elif read_only:
                    return QualificationResult(identity, feature, spec.revision, False, "unknown", "qualification has not run")
                elif identity.version in spec.deny_versions:
                    rows[feature] = {"ok": False, "source": "failed", "reason": spec.deny_versions[identity.version], "facts": {}}
                elif spec.platforms and identity.platform not in spec.platforms:
                    rows[feature] = {"ok": False, "source": "failed", "reason": "feature is not qualified on this platform", "facts": {}}
                elif (spec.seeds.get(identity.version, {}).get(feature) and not refresh
                      and (not spec.seed_platforms or identity.platform in spec.seed_platforms)):
                    for sibling, other in specs.items():
                        if (sibling not in rows and other.revision == spec.revision and other.seeds.get(identity.version, {}).get(sibling)
                                and identity.version not in other.deny_versions
                                and (not other.platforms or identity.platform in other.platforms)
                                and (not other.seed_platforms or identity.platform in other.seed_platforms)):
                            rows[sibling] = {"ok": True, "source": "seeded", "reason": "trusted maintainer version/platform seed; binary provenance was not measured", "facts": _feature_facts(sibling, other.seeds[identity.version], specs)}
                else:
                    measured = _probe(spec, identity)
                    # Reject even partial success if the executable changed during measurement.
                    try:
                        stable = identify(backend) == identity
                    except (OSError, UnsupportedCapability):
                        stable = False
                    if not stable:
                        measured = ProbeResult(False, "installed binary changed or became unavailable during qualification")
                    successes = dict(measured.features)
                    if measured.ok and measured.facts.get(feature):
                        successes.setdefault(feature, measured.facts)
                    failures = dict(measured.failures)
                    if feature not in successes:
                        failures.setdefault(feature, measured.reason if not measured.ok else "probe did not establish the requested feature")
                    # The shared callback has completed: persist all reported results, and mark
                    # every omitted sibling unavailable so another launch cannot silently retry it.
                    for sibling, other in specs.items():
                        if other.revision != spec.revision or other.probe is not spec.probe:
                            continue
                        facts = successes.get(sibling)
                        blocked = other.deny_versions.get(identity.version)
                        if other.platforms and identity.platform not in other.platforms:
                            blocked = "feature is not qualified on this platform"
                        if blocked or sibling in failures:
                            rows[sibling] = {"ok": False, "source": "failed", "reason": (blocked or failures[sibling])[:1000], "facts": {}}
                        elif isinstance(facts, dict) and facts.get(sibling):
                            rows[sibling] = {"ok": True, "source": "auto-probed", "reason": measured.reason[:1000], "facts": _feature_facts(sibling, facts, specs)}
                        else:
                            rows[sibling] = {"ok": False, "source": "failed", "reason": "probe bundle did not establish this feature", "facts": {}}
                _write(path, document, directory_fd)
                return _result(identity, spec, rows[feature], rows, specs)
            finally:
                if lock is not None:
                    os.close(lock)
                os.close(directory_fd)
    except (OSError, ValueError, RecursionError) as exc:
        raise UnsupportedCapability(f"qualification state unavailable: {exc}") from exc


def require(backend, feature: str, *, refresh=False, root=None) -> QualificationResult:
    result = _operate(backend, feature, refresh=refresh, root=root)
    if not result.ok:
        raise UnsupportedCapability(f"{backend.name} {feature} qualification failed: {result.reason} (version {result.identity.version}; source {result.source}); run harflow-ctl qualify {backend.name} to retry")
    return result


def inspect(backend, *, root=None, identity: Identity | None = None) -> dict[str, Any]:
    """Cached-only inspection; never starts a provider probe."""
    identity = identity or identify(backend)
    return {feature: _operate(backend, feature, root=root, read_only=True, identity=identity).as_dict() for feature in backend.qualification_features()}


def invalidate(backend, feature: str, reason: str, *, root=None, identity=None, expected_version=None) -> QualificationResult:
    if identity is None and expected_version is not None:
        identity = identity_for(backend, expected_version)
        if identity is None:
            raise UnsupportedCapability("cannot invalidate qualification without the original launch identity")
    return _operate(backend, feature, root=root, failure=reason, identity=identity)
