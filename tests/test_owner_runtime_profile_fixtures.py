"""Standalone tests for the synthetic owner-runtime candidate corpus.

Run with ``.venv/bin/python -m unittest discover -s tests -p
test_owner_runtime_profile_fixtures.py``. These tests deliberately avoid the
product test configuration and all runtime/backend interfaces.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "generate-owner-runtime-profile-fixtures.py"
PACKAGE = ROOT / "scripts"
SPECS = (
    ROOT / "docs" / "decisions" / "owner-runtime-v1.md",
    ROOT / "docs" / "decisions" / "owner-runtime-v1-schema.md",
)
TEMP_ROOT = Path(tempfile.gettempdir()).resolve()

sys.path.insert(0, str(PACKAGE))
from owner_runtime_profile import Rejected, classify, decode, fixtures  # noqa: E402
from owner_runtime_profile import tooling  # noqa: E402


class OwnerRuntimeProfileFixturesTests(unittest.TestCase):
    def _generate(self, out: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--output-dir", str(out)],
            cwd=ROOT, text=True, capture_output=True, timeout=15, check=False,
        )

    def test_manifest_is_exact_inventory_and_repeatable(self) -> None:
        with tempfile.TemporaryDirectory(dir=TEMP_ROOT) as td:
            first, second = Path(td) / "a", Path(td) / "b"
            for out in (first, second):
                result = self._generate(out)
                self.assertEqual(result.returncode, 0, result.stderr)
            names = sorted(p.name for p in first.iterdir())
            self.assertEqual(names, sorted(p.name for p in second.iterdir()))
            for name in names:
                self.assertEqual((first / name).read_bytes(), (second / name).read_bytes())
            manifest_bytes = (first / "manifest.json").read_bytes()
            manifest = json.loads(manifest_bytes)
            self.assertEqual(manifest["format"], "harflow.owner-runtime.profile.component")
            self.assertEqual(manifest["status"], "specified_unimplemented")
            self.assertEqual(manifest["provenance"], "synthetic_specification")
            self.assertIs(manifest["python_runtime_support"], False)
            rows = manifest["files"]
            self.assertGreater(len(rows), 0)
            self.assertLessEqual(len(rows), 64)
            self.assertEqual(len({r["filename"] for r in rows}), len(rows))
            self.assertEqual(set(names), {"manifest.json", "manifest.sha256"} |
                             {r["filename"] for r in rows})
            for row in rows:
                payload = (first / row["filename"]).read_bytes()
                self.assertEqual(len(payload), row["bytes"])
                self.assertEqual(hashlib.sha256(payload).hexdigest(), row["sha256"])
                self.assertEqual(classify(payload), row["expected"])
            digest = hashlib.sha256(manifest_bytes).hexdigest().encode() + b"\n"
            self.assertEqual((first / "manifest.sha256").read_bytes(), digest)
            self.assertEqual(len(manifest["specifications"]), 2)
            for spec, entry in zip(SPECS, manifest["specifications"]):
                self.assertEqual(entry["sha256"], hashlib.sha256(spec.read_bytes()).hexdigest())

    def test_check_is_read_only_and_detects_content_inventory_and_link_changes(self) -> None:
        with tempfile.TemporaryDirectory(dir=TEMP_ROOT) as td:
            out = Path(td) / "corpus"
            result = self._generate(out)
            self.assertEqual(result.returncode, 0, result.stderr)
            before_metadata = {p.name: (p.stat().st_ino, p.stat().st_mtime_ns,
                                        p.stat().st_mode, p.stat().st_size) for p in out.iterdir()}
            check = subprocess.run(
                [sys.executable, str(SCRIPT), "--check", "--output-dir", str(out)],
                cwd=ROOT, text=True, capture_output=True, timeout=15, check=False,
            )
            self.assertEqual(check.returncode, 0, check.stderr)
            self.assertEqual(before_metadata, {p.name: (p.stat().st_ino, p.stat().st_mtime_ns,
                                                         p.stat().st_mode, p.stat().st_size)
                                               for p in out.iterdir()})
            before = {p.name: p.read_bytes() for p in out.iterdir() if p.is_file()}
            fixture = next(p for p in out.glob("*.json") if p.name not in
                           {"manifest.json", "manifest.sha256"})
            original = fixture.read_bytes()
            fixture.write_bytes(original + b" ")
            self.assertNotEqual(self._check(out).returncode, 0)
            fixture.write_bytes(original)
            extra = out / "unlisted.json"
            extra.write_bytes(b"{}")
            self.assertNotEqual(self._check(out).returncode, 0)
            extra.unlink()
            fixture.unlink()
            self.assertNotEqual(self._check(out).returncode, 0)
            fixture.write_bytes(original)
            self.assertEqual(self._check(out).returncode, 0)
            self.assertEqual(before, {p.name: p.read_bytes() for p in out.iterdir() if p.is_file()})

    def _check(self, out: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--check", "--output-dir", str(out)],
            cwd=ROOT, text=True, capture_output=True, timeout=15, check=False,
        )

    def test_tool_refuses_symlinks_traversal_and_candidate_replacement(self) -> None:
        with tempfile.TemporaryDirectory(dir=TEMP_ROOT) as td:
            out = Path(td) / "corpus"
            self.assertEqual(self._generate(out).returncode, 0)
            target = Path(td) / "outside"
            target.write_bytes(b"untouched")
            manifest = json.loads((out / "manifest.json").read_bytes())
            name = manifest["files"][0]["filename"]
            victim = out / name
            original = victim.read_bytes()
            victim.unlink()
            victim.symlink_to(target)
            with self.assertRaises((ValueError, OSError)): tooling.check(out, SPECS)
            self.assertEqual(target.read_bytes(), b"untouched")
            victim.unlink()
            victim.write_bytes(original)
            manifest["files"][0]["filename"] = "../outside"
            manifest_bytes = (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode()
            (out / "manifest.json").write_bytes(manifest_bytes)
            (out / "manifest.sha256").write_text(hashlib.sha256(manifest_bytes).hexdigest() + "\n")
            with self.assertRaises((ValueError, OSError)): tooling.check(out, SPECS)
            self.assertEqual(target.read_bytes(), b"untouched")

    def test_generation_is_atomic_for_regular_files_and_refuses_hardlink_aliases(self) -> None:
        with tempfile.TemporaryDirectory(dir=TEMP_ROOT) as td:
            out = Path(td) / "corpus"
            self.assertEqual(self._generate(out).returncode, 0)
            before = {p.name: p.read_bytes() for p in out.iterdir()}
            self.assertEqual(self._generate(out).returncode, 0)
            self.assertEqual(before, {p.name: p.read_bytes() for p in out.iterdir()})
            victim = out / next(name for name in before if name.endswith(".json") and name != "manifest.json")
            outside = Path(td) / "outside-sentinel"
            os.link(victim, outside)
            inode = outside.stat().st_ino
            self.assertEqual(outside.stat().st_nlink, 2)
            refused = self._generate(out)
            self.assertNotEqual(refused.returncode, 0)
            self.assertEqual(outside.read_bytes(), before[victim.name])
            self.assertEqual(outside.stat().st_ino, inode)
            self.assertEqual(outside.stat().st_nlink, 2)

    def test_validator_corpus_has_independent_known_model_oracles(self) -> None:
        corpus = fixtures()
        self.assertGreaterEqual(len(corpus), 20)
        self.assertLessEqual(len(corpus), 64)
        by_name = {name: (raw, expected) for name, (raw, expected) in corpus.items()}
        kinds = set()
        for raw, _ in corpus.values():
            try:
                kind = json.loads(raw).get("kind")
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if kind in {"policy", "permission", "observation", "storage", "steering_guard"}:
                kinds.add(kind)
        self.assertEqual(kinds, {"policy", "permission", "observation", "storage", "steering_guard"})
        required_cases = {
            "policy-default.json", "policy-explicit-boundary.json", "policy-unknown-flag.json",
            "policy-bool.json", "policy-future-and-bad.json", "permission-opt-in.json",
            "permission-structure-before-enum.json", "observation-sticky-failure.json",
            "observation-open-turn.json", "observation-unknown-required.json",
            "observation-incidental-pending.json", "storage-refused-old-revision.json",
            "storage-unknown-null-revision.json", "storage-committed-null.json",
            "storage-overflow.json", "steering-observer_attachment.json",
            "binding-missing-before-future.json", "encoding-duplicate.json",
            "encoding-nonfinite.json", "encoding-utf8.json", "budget-bytes.json",
        }
        self.assertTrue(required_cases.issubset(by_name),
                        f"missing normative specimen cases: {sorted(required_cases - set(by_name))}")
        expected_decisions = {
            "policy-default.json": {"parse": "valid", "decision": "accept",
                                     "background_requirement": "required", "permission_mode": "os_enforced",
                                     "background_idle_seconds": 600, "tool_level_opt_in": False},
            "permission-opt-in.json": {"parse": "valid", "decision": "refuse",
                                        "reason": "opt_in_required"},
            "steering-agent_result.json": {"parse": "valid", "decision": "refuse",
                                            "reason": "not_steering_authority"},
        }
        for name, expected in expected_decisions.items():
            self.assertIn(name, by_name, f"missing manually specified oracle {name}")
            self.assertEqual(by_name[name][1], expected)
        for raw, expected in corpus.values():
            self.assertEqual(classify(raw), expected)

    def test_model_outputs_cannot_be_misread_as_product_capability(self) -> None:
        # Keep this candidate tooling detached from the Python engine and its v1 contract.
        source = SCRIPT.read_text(encoding="utf-8") + "\n" + "\n".join(
            p.read_text(encoding="utf-8") for p in PACKAGE.joinpath("owner_runtime_profile").glob("*.py")
        )
        forbidden = ("harflow_mcp.server", "backends.", "contracts/v1", "capabilities")
        for token in forbidden:
            self.assertNotIn(token, source)
        with tempfile.TemporaryDirectory(dir=TEMP_ROOT) as td:
            sentinel = Path(td) / "python-capability.json"
            sentinel.write_text('{"supported": false}\n', encoding="utf-8")
            before = sentinel.read_bytes()
            out = Path(td) / "fixtures"
            self.assertEqual(self._generate(out).returncode, 0)
            self.assertEqual(sentinel.read_bytes(), before)
            manifest = json.loads((out / "manifest.json").read_bytes())
            self.assertIs(manifest["python_runtime_support"], False)

    def test_envelope_shape_precedes_future_version_and_rejects_wrong_format(self) -> None:
        raw, expected = fixtures()["policy-default.json"]
        specimen = json.loads(raw)
        specimen.update(v=2, binding=None)
        self.assertEqual(classify(json.dumps(specimen).encode()),
                         {"parse": "invalid", "reason": "envelope"})
        specimen = json.loads(raw)
        specimen["format"] = "other.profile"
        self.assertEqual(classify(json.dumps(specimen).encode()),
                         {"parse": "invalid", "reason": "envelope"})
        specimen = json.loads(raw)
        specimen["data"]["unknown"] = 'escaped \\\" and braces {{{{{{{{ }}}}}}}}'
        self.assertEqual(classify(json.dumps(specimen).encode()), expected)

    def test_lexical_preflight_bounds_deep_json_and_preserves_encoding_rejections(self) -> None:
        deep = (b"[" * 200) + b"0" + (b"]" * 200)
        with self.assertRaises(Rejected) as raised:
            decode(deep)
        self.assertEqual(raised.exception.result, {"parse": "invalid", "reason": "budget"})
        for raw in (b'{"x":1,"x":2}', b'{"x":"\\ud800"}'):
            with self.subTest(raw=raw), self.assertRaises(Rejected) as raised:
                decode(raw)
            self.assertEqual(raised.exception.result,
                             {"parse": "invalid", "reason": "encoding"})

    def test_inventory_stops_at_first_entry_beyond_expected_set(self) -> None:
        names = set(tooling.expected_assets(SPECS))
        visited = 0

        class Entry:
            def __init__(self, name: str) -> None:
                self.name = name

            def is_symlink(self) -> bool:
                return False

            def is_file(self, *, follow_symlinks: bool = True) -> bool:
                return True

            def stat(self, *, follow_symlinks: bool = True):
                return mock.Mock(st_nlink=1)

        class Scanner:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def __iter__(self):
                nonlocal visited
                for name in sorted(names):
                    visited += 1
                    yield Entry(name)
                for index in range(100_000):
                    visited += 1
                    yield Entry(f"extra-{index}")

        with mock.patch.object(tooling.os, "scandir", return_value=Scanner()), \
                self.assertRaisesRegex(ValueError, "inventory"):
            tooling.inventory(Path(TEMP_ROOT), names, False)
        self.assertLessEqual(visited, len(names) + 1)

    def test_storage_process_identity_ignores_unknown_fields_but_not_pid(self) -> None:
        raw, advance = fixtures()["storage-acknowledged.json"]
        self.assertEqual(advance, {"parse": "valid", "decision": "advance", "reason": "acknowledged"})
        specimen = json.loads(raw)
        self.assertEqual(classify(json.dumps(specimen).encode()), advance)  # ack-only optional field
        del specimen["data"]["ack"]["writer"]["optional_note"]
        specimen["data"]["pending"]["writer"]["pending_note"] = True
        self.assertEqual(classify(json.dumps(specimen).encode()), advance)  # pending-only optional field
        specimen["data"]["ack"]["writer"]["pid"] += 1
        self.assertEqual(classify(json.dumps(specimen).encode()),
                         {"parse": "valid", "decision": "retain", "reason": "identity_mismatch"})

if __name__ == "__main__":
    unittest.main()
