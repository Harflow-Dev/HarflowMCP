import os
import pytest
from polybridge.extra_paths import normalize_paths, serialize_paths, restore_paths, revalidate_paths, check_child_paths


def test_identity_roundtrip_and_replacement(tmp_path):
    path = tmp_path / 'file'
    path.write_text('a')
    grants = normalize_paths([str(path)])
    assert restore_paths(serialize_paths(grants)) == grants
    revalidate_paths(grants)
    replacement = tmp_path / 'replacement'
    replacement.write_text('b')
    replacement.replace(path)
    with pytest.raises(ValueError, match='identity'):
        revalidate_paths(grants)


def test_symlink_resolution_duplicates_and_retarget(tmp_path):
    first = tmp_path / 'first'
    first.mkdir()
    second = tmp_path / 'second'
    second.mkdir()
    link = tmp_path / 'link'
    link.symlink_to(first)
    grant = normalize_paths([str(link)])
    assert grant[0].path == str(first)
    with pytest.raises(ValueError, match='duplicate'):
        normalize_paths([str(link), str(first)])
    link.unlink()
    link.symlink_to(second)
    with pytest.raises(ValueError):
        revalidate_paths(grant)


@pytest.mark.parametrize('value', [['relative'], ['~/foo'], ['/'], ['/missing-polybridge-path'], ['a\0'], [123], (), ['x'] * 17])
def test_invalid_inputs(value):
    with pytest.raises(ValueError):
        normalize_paths(value)


def test_special_file_and_type_change(tmp_path):
    fifo = tmp_path / 'fifo'
    os.mkfifo(fifo)
    with pytest.raises(ValueError, match='regular file'):
        normalize_paths([str(fifo)])
    path = tmp_path / 'directory'
    path.mkdir()
    grant = normalize_paths([str(path)])
    path.rmdir()
    path.write_text('changed')
    with pytest.raises(ValueError):
        revalidate_paths(grant)


def test_child_containment_preserves_file_scope(tmp_path):
    repo = tmp_path / 'repo'
    repo.mkdir()
    external = tmp_path / 'external'
    external.mkdir()
    file = external / 'file'
    file.touch()
    sibling = external / 'sibling'
    sibling.touch()
    check_child_paths(normalize_paths([str(external)]), normalize_paths([str(file)]), str(repo))
    check_child_paths(normalize_paths([str(file)]), normalize_paths([str(file)]), str(repo))
    with pytest.raises(ValueError, match='widen'):
        check_child_paths(normalize_paths([str(file)]), normalize_paths([str(external)]), str(repo))
    with pytest.raises(ValueError, match='widen'):
        check_child_paths(normalize_paths([str(file)]), normalize_paths([str(sibling)]), str(repo))
    check_child_paths((), normalize_paths([str(repo)]), str(repo))


@pytest.mark.parametrize('mutation', [lambda r: r.update(device=True), lambda r: r.update(kind='socket'), lambda r: r.update(extra=1), lambda r: r.update(path='/')])
def test_restore_strict_shape(tmp_path, mutation):
    record = serialize_paths(normalize_paths([str(tmp_path)]))[0]
    mutation(record)
    with pytest.raises(ValueError):
        restore_paths([record])


def test_utf8_and_aggregate_bounds(tmp_path):
    with pytest.raises(ValueError, match='4096'):
        normalize_paths(['/' + 'é' * 2048])
    # Requested spelling is bounded even when redundant components resolve to a short path.
    paths = []
    for index in range(5):
        child = tmp_path / str(index)
        child.mkdir()
        paths.append(str(tmp_path) + '/.' * 1800 + '/' + str(index))
    with pytest.raises(ValueError, match='16384'):
        normalize_paths(paths)


@pytest.mark.parametrize("markers,expected", [
    ({}, False),
    ({"extra_writable_paths": [], "extra_path_certification": None}, False),
    ({"extra_writable_paths": {}}, True),
    ({"extra_path_certification": False}, True),
    ({"enforcement": {"extra_writable_paths": None}}, True),
    ({"enforcement": {"extra_path_certification": ""}}, True),
])
def test_saved_authority_detection_accepts_mappings(markers, expected):
    from types import MappingProxyType, SimpleNamespace
    from polybridge.extra_paths import has_extra_path_authority
    if isinstance(markers.get("enforcement"), dict):
        markers = {**markers, "enforcement": MappingProxyType(markers["enforcement"])}
    assert has_extra_path_authority(MappingProxyType(markers)) is expected
    assert has_extra_path_authority(SimpleNamespace(**markers)) is expected
