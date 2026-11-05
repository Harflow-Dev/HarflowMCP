"""The complete producer bundle is reproducible without a provider CLI."""
from pathlib import Path
import runpy


def test_steering_generator_matches_canonical_bytes_twice():
    root = Path(__file__).parents[1]
    generate = runpy.run_path(str(root / 'scripts/generate-steering-fixtures.py'))['generate']
    first = generate()
    second = generate()
    assert first == second
    for path, value in first.items():
        assert path.read_bytes() == value, str(path.relative_to(root))
