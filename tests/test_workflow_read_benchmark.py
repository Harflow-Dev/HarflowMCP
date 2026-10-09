"""Read runner safety and fixture reuse, without paid agent execution."""
import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'benchmark-workflow-reads.py'
spec = importlib.util.spec_from_file_location('workflow_read_benchmark', SCRIPT)
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


def test_worker_refuses_mutation_before_import(tmp_path, capsys):
    assert benchmark.worker(tmp_path / 'missing-source', ['workflow-run', 'private']) == 2
    assert capsys.readouterr().err == 'Benchmark fixture permits read commands only\n'


def test_existing_fixture_sizes_and_scenarios_are_reused():
    source = SCRIPT.parents[1]
    baseline = benchmark.load_baseline(source)
    assert baseline.SIZES == {'small': (12, 24, 32), 'large': (240, 400, 1200)}
    assert callable(baseline.benchmark)
    assert callable(baseline.scenario)
