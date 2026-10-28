"""Initial dispatch protects existing edits before granting branch transitions."""
import subprocess

import pytest

from harflow_mcp.command_grants import CommandGrantError, check_branch_preparation


@pytest.fixture
def checkout(tmp_path):
    repo = tmp_path / 'app'
    repo.mkdir()
    subprocess.run(['git', 'init', '-b', 'main', str(repo)], check=True, capture_output=True)
    (repo / 'tracked').write_text('original')
    subprocess.run(['git', 'add', 'tracked'], cwd=repo, check=True)
    subprocess.run(['git', '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-m', 'seed'], cwd=repo, check=True, capture_output=True)
    return repo


@pytest.mark.parametrize('command', ['git switch main', 'git switch -c feature/prep main', 'git pull --ff-only origin main'])
@pytest.mark.parametrize('kind', ['tracked', 'untracked', 'staged'])
def test_dirty_transition_refused_without_changing_checkout(checkout, command, kind):
    changed = checkout / ('new' if kind == 'untracked' else 'tracked')
    changed.write_text('preserve me')
    if kind == 'staged':
        subprocess.run(['git', 'add', '.'], cwd=checkout, check=True)
    with pytest.raises(CommandGrantError, match='inspect git status.*preserve'):
        check_branch_preparation([command], checkout)
    assert changed.read_text() == 'preserve me'
    assert subprocess.check_output(['git', 'branch', '--show-current'], cwd=checkout).strip() == b'main'


def test_clean_contained_transition_and_fetch_only_dirty(checkout):
    check_branch_preparation(['git -C app switch main'], checkout.parent)
    (checkout / 'new').write_text('preserve')
    check_branch_preparation(['git -C app fetch origin main'], checkout.parent)


def test_unreadable_target_refuses(tmp_path):
    with pytest.raises(CommandGrantError, match='accessible Git checkout'):
        check_branch_preparation(['git switch main'], tmp_path)
