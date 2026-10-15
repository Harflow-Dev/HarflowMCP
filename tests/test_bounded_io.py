import os

import pytest

from polybridge.bounded_io import ReadLimit, read_json
from polybridge.catalog import MetadataBudget


def test_malformed_input_charges_bytes_and_decode(tmp_path):
    path = tmp_path / 'metadata'
    path.write_bytes(b'{')
    budget = MetadataBudget()
    with pytest.raises(ValueError):
        read_json(path, 100, budget=budget)
    assert (budget.metadata_bytes, budget.metadata_decodes) == (1, 1)


@pytest.mark.parametrize('kind', ['symlink', 'fifo', 'directory'])
def test_nonregular_input_is_rejected_before_read(tmp_path, kind):
    path = tmp_path / 'metadata'
    if kind == 'symlink':
        target = tmp_path / 'target'
        target.write_text('{}')
        path.symlink_to(target)
    elif kind == 'fifo':
        os.mkfifo(path)
    else:
        path.mkdir()
    with pytest.raises((OSError, ReadLimit)):
        read_json(path, 100)


def test_decoder_recursion_failure_is_normalized_and_charged(tmp_path, monkeypatch):
    from polybridge import bounded_io
    path = tmp_path / 'metadata'
    raw = b'[[[0]]]'
    def exhausted_decoder(content):
        raise RecursionError('decoder nesting limit')
    monkeypatch.setattr(bounded_io.json, 'loads', exhausted_decoder)
    path.write_bytes(raw)
    budget = MetadataBudget()
    with pytest.raises(ReadLimit):
        read_json(path, len(raw), budget=budget)
    assert (budget.metadata_bytes, budget.metadata_decodes) == (len(raw), 1)


def test_growth_after_initial_stat_charges_read_bytes(tmp_path, monkeypatch):
    from polybridge import bounded_io
    path = tmp_path / 'metadata'
    path.write_bytes(b'{}')
    original = bounded_io.os.fstat
    calls = 0
    def growing(fd):
        nonlocal calls
        info = original(fd)
        calls += 1
        if calls == 1:
            with path.open('ab') as output:
                output.write(b' ' * 20)
        return info
    monkeypatch.setattr(bounded_io.os, 'fstat', growing)
    budget = MetadataBudget()
    with pytest.raises(ReadLimit):
        read_json(path, 10, budget=budget)
    assert budget.metadata_bytes == 10
    assert budget.metadata_decodes == 0
