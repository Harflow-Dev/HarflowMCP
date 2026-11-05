"""The ST04 producer bundle comes from reproducible isolated service calls."""
from pathlib import Path
import runpy


def test_replan_generator_matches_canonical_bytes_twice():
    root = Path(__file__).parents[1]
    generate = runpy.run_path(str(root / 'scripts/generate-replan-fixtures.py'))['generate']
    first, second = generate(), generate()
    assert first == second
    for path, value in first.items():
        assert path.read_bytes() == value, str(path.relative_to(root))
