"""Dispatch-time `allowed_commands`: bounds, the Claude grammar, argv composition, tamper refusal and disclosure."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from harflow_mcp import backends, command_grants
from harflow_mcp.backends import claude_commands, get
from harflow_mcp.backends.base import Enforcement, UnsupportedCapability, nested_enforcement_violation
from harflow_mcp.backends.claude import DISALLOWED_TOOLS, ClaudeBackend, UnsafeInvocationError
from harflow_mcp.backends.claude_approvals import check_event
from harflow_mcp.command_grants import CommandGrantError
from harflow_mcp.task_documents import TaskDocumentError, qualify_enforcement

SESSION = "11111111-1111-4111-8111-111111111111"
CLAUDE = ClaudeBackend()
PUBLISH_ENTRIES = "Bash(git commit:*),Bash(git push:*),Bash(gh pr create:*),Bash(gh pr edit:*),Bash(gh pr view:*),Bash(gh pr comment:*),Bash(gh pr review:*),Bash(gh pr checks:*),Bash(gh pr diff:*),Bash(gh pr list:*),Bash(gh repo view:*)"


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    (root / "src").mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "escape").symlink_to(outside)
    (outside / "file").write_text("x")
    (root / "link=name").symlink_to(outside / "file")
    return root


def build(repo: Path, freedom: str = "write_in_repo", patterns: tuple[str, ...] = (), *, resume: bool = False, **kwargs):
    arguments = dict(repo=repo, freedom=freedom, model=None, max_turns=None, reasoning_effort=None)
    if resume:
        invocation = CLAUDE.build_resume_argv("x", session_id=SESSION, **arguments)
    else:
        invocation = CLAUDE.build_start_argv("x", session_id=SESSION, **arguments)
    invocation = CLAUDE.configure_allowed_commands(invocation, patterns, freedom, repo, "2.1.310")
    CLAUDE.assert_safe(invocation, freedom)
    return invocation


def flag(argv: list[str], name: str) -> list[str]:
    return [argv[i + 1] for i, token in enumerate(argv) if token == name]


# --- generic bounds -------------------------------------------------------------------------------

def test_bounds_accept_a_normal_list():
    assert command_grants.check_bounds(["make test:*", "cargo build"]) == ("make test:*", "cargo build")
    assert command_grants.normalize(None) is None
    assert command_grants.normalize([]) == ()


@pytest.mark.parametrize("patterns,reason", [
    ("make test", "list of strings"),
    ([1], "strings"),
    ([""], "empty"),
    ([" make"], "whitespace"),
    (["make "], "whitespace"),
    (["make\ntest"], "control"),
    (["make\x00"], "control"),
    (["make test"], "control"),
    (["make test", "make test"], "duplicated"),
    ([f"p{i}" for i in range(17)], "at most 16"),
    (["a" * 257], "257 bytes"),
    ([("a" * 250) + str(i) for i in range(9)], "total size"),
    (["é" * 129], "258 bytes"),
])
def test_bounds_refuse(patterns, reason):
    with pytest.raises(CommandGrantError, match=reason):
        command_grants.check_bounds(patterns)


def test_bounds_allow_exactly_the_limits():
    assert len(command_grants.check_bounds([f"p{i}" for i in range(16)])) == 16
    assert command_grants.check_bounds(["a" * 256])
    assert command_grants.check_bounds([("a" * 255) + str(i) for i in range(8)])


# --- Claude grammar: accepted ---------------------------------------------------------------------

ACCEPTED = [
    "python3 -m pytest:*",
    "python3 -m pytest *",
    "python3 -m pytest tests/test_x.py",
    "python -m unittest:*",
    "python3.12 scripts/build.py",
    "node scripts/build.js",
    "make test:*",
    "make build",
    "cargo build:*",
    "go test ./...",
    "npm run test:*",
    "pnpm run lint",
    "npx prettier --check src:*",
    "uv run pytest tests:*",
    "uv run python3 -m pytest:*",
    "uvx ruff check:*",
    "cp a b",
    "touch src/new.txt",
    "mkdir src/pkg",
    "find src -name x",
    "tuist install",
    "yarn node scripts/x.js",
    "pnpm test",
    "pnpm run lint",
    "pnpm install",
    "node16 scripts/x.js",
    "cmake --build build",
    "npm --prefix src install",
    "uv --directory src run pytest tests:*",
    "yarn --cwd=src build:*",
    "yarn tsc --noEmit",
    "yarn tsc:*",
    "npm test:*",
    "npm run build",
    "npm ci",
    "uv sync",
    "uv pip install requests",
    "poetry install",
    "bundle install",
]


@pytest.mark.parametrize("pattern", ACCEPTED)
def test_grammar_accepts(pattern, repo):
    claude_commands.validate(pattern, "write_in_repo", repo)
    claude_commands.validate(pattern, "publish", repo)


def test_grammar_accepts_the_issue_examples_rewritten(repo):
    for pattern in (f"yarn --cwd {repo} tsc:*", f"mvn -f {repo}/pom.xml test:*", f"mise exec -C {repo} -- tuist install",
                    f"mise exec -C {repo} -- tuist generate:*", f"yarn --cwd={repo} build:*"):
        claude_commands.validate(pattern, "write_in_repo", repo)


def test_gh_is_publish_only_with_a_fixed_pair_list(repo):
    for pattern in ("gh workflow run:*", "gh workflow run build.yml", "gh pr create", "gh pr view 12", "gh run watch:*", "gh pr checks:*"):
        claude_commands.validate(pattern, "publish", repo)
        with pytest.raises(CommandGrantError, match="gh is refused below publish"):
            claude_commands.validate(pattern, "write_in_repo", repo)
    for pattern in ("gh api repos/x/y", "gh pr merge 1", "gh pr:*", "gh repo delete x", "gh auth login"):
        with pytest.raises(CommandGrantError, match="gh is accepted only as one of"):
            claude_commands.validate(pattern, "publish", repo)


# --- Claude grammar: refused ----------------------------------------------------------------------

REFUSED = [
    # git is refused everywhere, in every spelling that could reach the program position
    ("git status", "git is refused"),
    ("git commit:*", "git is refused"),
    ("/usr/bin/git status", "lowercase bare name"),
    ("./git status", "lowercase bare name"),
    ('"git" status', "not an allowed character"),
    ("g\\it status", "not an allowed character"),
    ("GIT status", "lowercase bare name"),
    ("GIT_DIR=x git status", "lowercase bare name"),
    ("env git status", "shell or command evaluator"),
    ("/bin/bash -c*", "never inside a token"),
    ("/bin/bash -c x", "lowercase bare name"),
    ("bash -c x", "shell or command evaluator"),
    ("sh script.sh", "shell or command evaluator"),
    # interpreters
    ("python3 -cX", "interpreter 'python3' is accepted only"),
    ("python3 -c x", "interpreter 'python3' is accepted only"),
    ("python3 -Ic", "interpreter 'python3' is accepted only"),
    ("python3 -m", "literal module"),
    ("python3 -m code:*", "evaluates code"),
    ("python3 -m runpy:*", "evaluates code"),
    ("python3", "interpreter 'python3' is accepted only"),
    ("python3 x", "interpreter 'python3' is accepted only"),
    ("node -pe", "interpreter 'node' is accepted only"),
    ("node -e x", "interpreter 'node' is accepted only"),
    ("node --eval x", "interpreter 'node' is accepted only"),
    ("deno eval x", "interpreter 'deno' is accepted only"),
    ("perl -e x", "program that evaluates code"),
    ("ruby x", "program that evaluates code"),
    # wildcards
    ("pytest*", "never inside a token"),
    ("pytest *", "needs a literal non-option token"),
    ("pytest:*", "needs a literal non-option token"),
    ("make:*", "needs a literal non-option token"),
    ("tsc --noEmit:*", "needs a literal non-option token"),
    ("make test*", "never inside a token"),
    ("make * test", "never inside a token"),
    ("make test:* *", "at most one"),
    ("npx *", "wildcard cannot replace the program"),
    ("uv run *", "wildcard cannot replace the program"),
    ("a b:*:*", "at most one"),
    # launchers, recursively
    ("uv run python3 -c x", "interpreter 'python3' is accepted only"),
    ("npx bash x", "shell or command evaluator"),
    ("npx git status", "git is refused"),
    ("uv run git commit", "git is refused"),
    ("mise exec -C x *", "followed by `--`"),
    ("mise exec -C x -- bash x", "shell or command evaluator"),
    ("mise exec -C x --", "launched command after"),
    ("npx", "needs a launched command"),
    ("npm exec", "needs a launched command"),
    ("npx npx npx npx tool sub", "nest at most 2"),
    ("uv run npx uvx tool sub", "nest at most 2"),
    ("npx prettier -- x", "launcher separator"),
    # wrappers, privilege, system, containers
    ("nice -n 5:*", "wrapper that runs other commands"),
    ("arch -arm64:*", "wrapper that runs other commands"),
    ("caffeinate -i make test", "wrapper that runs other commands"),
    ("time make test", "wrapper that runs other commands"),
    ("xargs rm", "wrapper that runs other commands"),
    ("sudo make install", "privilege escalation"),
    ("ssh host ls", "privilege escalation"),
    ("kill 1", "system state"),
    ("sed -i x y", "program that evaluates code"),
    ("awk x", "program that evaluates code"),
    ("docker run img *", "container, remote and flake launchers"),
    ("docker build .", "container, remote and flake launchers"),
    ("podman run x y", "container, remote and flake launchers"),
    ("kubectl get pods", "container, remote and flake launchers"),
    ("nix run x", "container, remote and flake launchers"),
    # find, make
    ("find . -exec x", "find is refused"),
    ("find . -delete", "find is refused"),
    ("find . -name x:*", "find is refused"),
    ("find . -fprint x", "find is refused"),
    ("make", "literal target"),
    ("make -j4", "literal target"),
    ("make --eval=x test", "evaluate makefile text"),
    ("make -E x test", "evaluate makefile text"),
    # paths
    ("cp a /etc/x", "resolves outside"),
    ("cp a ../x", "`..` segments"),
    ("cp a *", "accepts only exact patterns"),
    ("mv a *", "accepts only exact patterns"),
    ("tee x:*", "accepts only exact patterns"),
    ("cp a escape", "resolves outside"),
    ("cp escape x", "resolves outside"),
    ("cp a escape/x", "resolves outside"),
    ("touch -tescape", "resolves outside"),
    ("cp --target-directory=escape a", "resolves outside"),
    ("rm -rf *", "needs a literal non-option token"),
    ("rm -rf /", "resolves outside"),
    ("chmod 777 /etc/passwd", "resolves outside"),
    ("mvn -f/etc/x test:*", "contains / but is not a plain path"),
    ("mvn -I/x test:*", "contains / but is not a plain path"),
    ("make key=/x test", "contains / but is not a plain path"),
    ("make --file=/etc/x test", "resolves outside"),
    ("make test ../x", "`..` segments"),
    ("make test ..", "`..` segments"),
    ("make test src//x", "empty or `..` segments"),
    ("curl http://example.com", "empty or `..` segments"),
    ("make test escape/x", "resolves outside"),
    ("mvn -f /tmp/pom.xml test:*", "resolves outside"),
    ("npx prettier --write=/etc/x file", "resolves outside"),
    ("mvn --foo=bar=/x test", "contains / but is not a plain path"),
    # characters and spacing
    ("make test,make build", "comma"),
    ("a,b", "comma"),
    ("make 'test'", "not an allowed character"),
    ("make $(x)", "not an allowed character"),
    ("make `x`", "not an allowed character"),
    ("make test; rm x", "not an allowed character"),
    ("make test && make build", "not an allowed character"),
    ("make test | cat", "not an allowed character"),
    ("make test > out", "not an allowed character"),
    ("make ~/x", "not an allowed character"),
    ("make test  build", "single spaces"),
    ("make -- test", "launcher separator"),
    # options before a launcher subcommand
    ("uv --offline run git commit -m x", "before the subcommand"),
    ("uv --quiet run bash x", "before the subcommand"),
    ("npm --offline exec bash", "before the subcommand"),
    ("npm --yes exec *", "before the subcommand"),
    ("uv --directory . run *", "wildcard cannot replace the program"),
    ("yarn --cwd . exec *", "wildcard cannot replace the program"),
    ("pnpm --dir . exec *", "wildcard cannot replace the program"),
    ("pnpm --silent dlx x", "before the subcommand"),
    ("uv --quiet run gh pr merge 1", "before the subcommand"),
    ("uv --directory /etc run pytest", "resolves outside"),
    ("uv --directory", "needs a directory value"),
    ("npm --prefix ../x install", "`..` segments"),
    ("uv --directory . run bash x", "shell or command evaluator"),
    # exact-only key=value operands
    ("dd if=a of=escape", "resolves outside"),
    ("dd if=a of=/etc/x", "contains / but is not a plain path"),
    # interpreter families, launchers and programs the first list missed
    ("node16 -e x", "interpreter 'node16' is accepted only"),
    ("nodejs -e x", "interpreter 'nodejs' is accepted only"),
    ("pypy3 -c x", "interpreter 'pypy3' is accepted only"),
    ("tsx -e x", "interpreter 'tsx' is accepted only"),
    ("ts-node -e x", "interpreter 'ts-node' is accepted only"),
    ("yarn node -e x", "interpreter 'node' is accepted only"),
    ("pdm run bash x", "shell or command evaluator"),
    ("hatch run bash x", "shell or command evaluator"),
    ("pipenv run bash x", "shell or command evaluator"),
    ("conda run bash x", "shell or command evaluator"),
    ("pixi run bash x", "shell or command evaluator"),
    ("rye run bash x", "shell or command evaluator"),
    ("npm explore bash x", "is not accepted"),
    ("npm explore x bash *", "is not accepted"),
    ("npm explore x sh", "is not accepted"),
    ("npm init bash", "is not accepted"),
    ("npm create bash", "is not accepted"),
    ("yarn create bash", "is not accepted"),
    ("pnpm create bash", "is not accepted"),
    ("uv tool --offline run --from pip python -c pass", "is not accepted"),
    ("uv tool run x", "is not accepted"),
    ("npm --prefix . *", "needs a literal subcommand"),
    ("npm --prefix .:*", "needs a literal subcommand"),
    ("uv --directory . *", "needs a literal subcommand"),
    ("yarn --cwd . *", "needs a literal subcommand"),
    ("pnpm --dir . *", "needs a literal subcommand"),
    ("npm run *", "needs a literal script name"),
    ("npm run:*", "needs a literal script name"),
    ("yarn run:*", "needs a literal script name"),
    ("yarn bash x", "shell or command evaluator"),
    ("yarn git status", "git is refused"),
    ("yarn rm:*", "accepts only exact patterns"),
    ("yarn python3:*", "interpreter 'python3' is accepted only"),
    ("npm unknown", "is not accepted"),
    ("uv tool", "is not accepted"),
    ("touch link=name", "resolves outside"),
    ("cp source link=name", "resolves outside"),
    ("ssh-agent x", "privilege escalation or remote execution"),
    ("vim x", "program that evaluates code"),
    ("vi x", "program that evaluates code"),
    ("nvim x", "program that evaluates code"),
    ("ex x", "program that evaluates code"),
    ("ed x", "program that evaluates code"),
    ("emacs x", "program that evaluates code"),
    ("nano x", "program that evaluates code"),
    ("valgrind x", "wrapper that runs other commands"),
    ("cross-env x", "wrapper that runs other commands"),
    ("direnv x", "wrapper that runs other commands"),
    ("cmake -E x", "cmake -E and -P"),
    ("cmake -P x", "cmake -E and -P"),
    ("git-lfs x", "git is refused"),
    ("pnpm foo", "is not accepted"),
    ("pnpm run:*", "literal script name"),
    ("pnpm run *", "literal script name"),
]


@pytest.mark.parametrize("pattern,reason", REFUSED)
def test_grammar_refuses_and_names_pattern_and_reason(pattern, reason, repo):
    with pytest.raises(CommandGrantError) as raised:
        claude_commands.validate(pattern, "publish", repo)
    assert reason in str(raised.value)
    assert repr(pattern) in str(raised.value)


def test_grammar_refuses_a_stray_double_dash_and_a_comma_by_name(repo):
    with pytest.raises(CommandGrantError, match="comma would split"):
        claude_commands.validate("make test,build", "write_in_repo", repo)
    with pytest.raises(CommandGrantError, match="launcher separator"):
        claude_commands.validate("cargo test -- --nocapture", "write_in_repo", repo)


def test_error_suggests_the_colon_form():
    with pytest.raises(CommandGrantError, match=r"pytest:\*"):
        claude_commands.validate("pytest*", "write_in_repo", None)


@pytest.mark.parametrize("freedom", ["read_only", "unrestricted", "bogus"])
def test_grammar_supports_only_write_in_repo_and_publish(freedom, repo):
    with pytest.raises(CommandGrantError, match="only at write_in_repo and publish"):
        claude_commands.validate("make test", freedom, repo)


def test_dd_operand_through_an_in_repo_symlink_to_an_outside_file_is_refused(repo):
    target = repo.parent / "outside" / "file"
    target.write_text("x")
    (repo / "out").symlink_to(target)
    with pytest.raises(CommandGrantError, match="resolves outside"):
        claude_commands.validate("dd if=a of=out", "write_in_repo", repo)
    claude_commands.validate("dd if=a of=src", "write_in_repo", repo)


def test_a_symlink_escaping_the_repository_is_refused_with_a_bare_name(repo):
    (repo / "bare").symlink_to(repo.parent / "outside")
    with pytest.raises(CommandGrantError, match="resolves outside"):
        claude_commands.validate("cp a bare", "write_in_repo", repo)
    with pytest.raises(CommandGrantError, match="resolves outside"):
        claude_commands.validate("cp a bare/x", "write_in_repo", repo)
    claude_commands.validate("cp a src", "write_in_repo", repo)
    claude_commands.validate(f"cp a {repo}/src/b", "write_in_repo", repo)
    with pytest.raises(CommandGrantError, match="resolves outside"):
        claude_commands.validate(f"cp a {repo.parent}/outside/b", "write_in_repo", repo)


def test_repo_free_validation_skips_only_path_resolution():
    claude_commands.validate("cp a /some/where/b", "write_in_repo", None)
    with pytest.raises(CommandGrantError):
        claude_commands.validate("cp a ../b", "write_in_repo", None)
    with pytest.raises(CommandGrantError):
        claude_commands.validate("bash x", "write_in_repo", None)


# --- backend seam and unsupported backends --------------------------------------------------------

def test_validate_allowed_commands_runs_bounds_then_grammar(repo):
    CLAUDE.validate_allowed_commands((), "read_only", repo)
    CLAUDE.validate_allowed_commands(("make test:*",), "write_in_repo", repo)
    with pytest.raises(CommandGrantError, match="duplicated"):
        CLAUDE.validate_allowed_commands(("make test", "make test"), "write_in_repo", repo)
    with pytest.raises(CommandGrantError, match="only at write_in_repo and publish"):
        CLAUDE.validate_allowed_commands(("make test",), "read_only", repo)


@pytest.mark.parametrize("name", ["codex", "opencode", "vibe", "antigravity", "cursor"])
def test_other_backends_refuse_with_their_reason(name, repo):
    backend = get(name)
    support = backend.allowed_commands_support()
    assert support["supported"] is False and name in support["reason"]
    backend.validate_allowed_commands((), "write_in_repo", repo)
    with pytest.raises(UnsupportedCapability, match=name):
        backend.validate_allowed_commands(("make test",), "write_in_repo", repo)
    with pytest.raises(UnsupportedCapability):
        backend.qualify_allowed_commands()
    with pytest.raises(UnsupportedCapability):
        backend.allowed_commands_enforcement(backend.enforcement("write_in_repo"), ("make test",))
    invocation = backend.build_start_argv("x", repo=repo, freedom="write_in_repo", session_id=SESSION if backend.capabilities.chooses_session_id else None,
                                          model=None, max_turns=None, reasoning_effort=None)
    with pytest.raises(UnsupportedCapability):
        backend.configure_allowed_commands(invocation, ("make test",), "write_in_repo", repo)
    assert backend.configure_allowed_commands(invocation, (), "write_in_repo", repo) is invocation
    with pytest.raises(UnsupportedCapability, match=name):
        backend.assert_safe(replace(invocation, allowed_commands=("make test",)), "write_in_repo")


def test_capability_metadata_reports_support(repo):
    metadata = backends.capability_metadata(CLAUDE)["allowed_commands"]
    assert metadata["supported"] is True and set(metadata["versions"]) == {"2.1.310", "2.1.312"}
    assert metadata["freedoms"] == ["write_in_repo", "publish"]
    assert (metadata["max_patterns"], metadata["max_pattern_bytes"], metadata["max_total_bytes"]) == (16, 256, 2048)
    assert "not a security boundary" in metadata["boundary"]
    for name in ("codex", "opencode", "vibe", "antigravity", "cursor"):
        assert backends.capability_metadata(get(name))["allowed_commands"]["supported"] is False


def test_qualification_uses_the_shared_framework(monkeypatch):
    from types import SimpleNamespace
    from harflow_mcp.backends import qualification
    calls = []
    def require(backend, feature):
        calls.append((backend, feature))
        return SimpleNamespace(identity=SimpleNamespace(version="9.9.9"))
    monkeypatch.setattr(qualification, "require", require)
    assert CLAUDE.qualify_allowed_commands() == "9.9.9"
    assert calls == [(CLAUDE, "allowed_commands")]
    def unavailable(backend, feature):
        raise UnsupportedCapability("isolated probe unavailable")
    monkeypatch.setattr(qualification, "require", unavailable)
    with pytest.raises(UnsupportedCapability, match="isolated probe unavailable"):
        CLAUDE.qualify_allowed_commands()


# --- argv composition and assert_safe -------------------------------------------------------------

def test_write_in_repo_passes_both_lists_and_the_deny_stays_unchanged(repo):
    invocation = build(repo, "write_in_repo", ("make test:*", "python3 -m pytest:*"))
    assert flag(invocation.argv, "--disallowedTools") == [DISALLOWED_TOOLS]
    assert flag(invocation.argv, "--allowedTools") == ["Bash(make test:*),Bash(python3 -m pytest:*)"]
    assert invocation.allowed_commands == ("make test:*", "python3 -m pytest:*")
    assert invocation.allowed_commands_version == "2.1.310"
    assert invocation.argv.index("--allowedTools") == invocation.argv.index("--disallowedTools") + 2


def test_publish_puts_the_freedoms_entries_first_and_has_no_deny(repo):
    invocation = build(repo, "publish", ("make test:*",))
    assert flag(invocation.argv, "--allowedTools") == [f"{PUBLISH_ENTRIES},Bash(make test:*)"]
    assert flag(invocation.argv, "--disallowedTools") == []


def test_without_grants_the_argv_is_unchanged(repo):
    plain = CLAUDE.build_start_argv("x", repo=repo, freedom="write_in_repo", session_id=SESSION, model=None, max_turns=None, reasoning_effort=None)
    assert build(repo, "write_in_repo", ()).argv == plain.argv
    assert flag(plain.argv, "--allowedTools") == []
    published = build(repo, "publish")
    assert flag(published.argv, "--allowedTools") == [PUBLISH_ENTRIES]


@pytest.mark.parametrize("freedom", ["read_only", "unrestricted"])
def test_other_freedoms_refuse_grants(repo, freedom):
    base = CLAUDE.build_start_argv("x", repo=repo, freedom=freedom, session_id=SESSION, model=None, max_turns=None, reasoning_effort=None)
    with pytest.raises(UnsupportedCapability, match="only at write_in_repo and publish"):
        CLAUDE.configure_allowed_commands(base, ("make test",), freedom, repo, "2.1.310")
    forged = replace(base, allowed_commands=("make test",), allowed_commands_version="2.1.310")
    with pytest.raises(UnsupportedCapability):
        CLAUDE.assert_safe(forged, freedom)


def test_resume_and_capped_shapes_compose_the_same_way(repo):
    resumed = build(repo, "write_in_repo", ("make test",), resume=True)
    assert flag(resumed.argv, "--allowedTools") == ["Bash(make test)"] and "--resume" in resumed.argv
    capped = CLAUDE.build_start_argv("x", repo=repo, freedom="publish", session_id=SESSION, model=None, max_turns=3, reasoning_effort=None)
    capped = CLAUDE.configure_allowed_commands(capped, ("make test",), "publish", repo, "2.1.310")
    CLAUDE.assert_safe(capped, "publish")
    assert flag(capped.argv, "--allowedTools") == [f"{PUBLISH_ENTRIES},Bash(make test)"]
    assert capped.argv[-2] == "--"


def test_configure_survives_the_writable_scratch_insertion(repo, tmp_path):
    invocation = build(repo, "write_in_repo", ("make test",))
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    with_scratch = CLAUDE.with_writable_directory(invocation, scratch, "write_in_repo")
    CLAUDE.assert_safe(with_scratch, "write_in_repo")
    assert with_scratch.allowed_commands == ("make test",)


def test_configure_refuses_a_second_application(repo):
    invocation = build(repo, "write_in_repo", ("make test",))
    with pytest.raises(ValueError, match="already carries"):
        CLAUDE.configure_allowed_commands(invocation, ("make build",), "write_in_repo", repo, "2.1.310")


def tamper(argv: list[str], name: str, value: str) -> list[str]:
    result = list(argv)
    result[result.index(name) + 1] = value
    return result


def test_assert_safe_accepts_exactly_that_composition_and_refuses_everything_else(repo):
    invocation = build(repo, "write_in_repo", ("make test:*",))
    for value in ("Bash(make test:*),Bash(make build)", "Bash(make test:*),Bash(git commit:*)", "Bash(make build)", "Bash(make test:*),",
                  f"{PUBLISH_ENTRIES},Bash(make test:*)", "Bash(make test:*) ", "Bash(*)", "Bash"):
        with pytest.raises(UnsafeInvocationError, match="--allowedTools was"):
            CLAUDE.assert_safe(replace(invocation, argv=tamper(invocation.argv, "--allowedTools", value)), "write_in_repo")
    with pytest.raises(UnsafeInvocationError, match="--allowedTools was"):
        CLAUDE.assert_safe(replace(invocation, allowed_commands=("make build",)), "write_in_repo")
    with pytest.raises(UnsafeInvocationError):
        CLAUDE.assert_safe(replace(invocation, argv=[*invocation.argv[:2], "--allowedTools", "Bash(x)", *invocation.argv[2:]]), "write_in_repo")
    with pytest.raises(UnsafeInvocationError, match="--disallowedTools was"):
        CLAUDE.assert_safe(replace(invocation, argv=tamper(invocation.argv, "--disallowedTools", "Bash(git commit:*)")), "write_in_repo")
    with pytest.raises(UnsafeInvocationError, match="allowed_tools|--allowedTools"):
        plain = CLAUDE.build_start_argv("x", repo=repo, freedom="write_in_repo", session_id=SESSION, model=None, max_turns=None, reasoning_effort=None)
        CLAUDE.assert_safe(replace(plain, argv=[*plain.argv[:2], "--allowedTools", "Bash(make test:*)", *plain.argv[2:]]), "write_in_repo")


def test_assert_safe_revalidates_the_grammar_and_requires_a_tuple_and_version(repo):
    invocation = build(repo, "write_in_repo", ("make test:*",))
    for forged in ("git commit:*", "bash x", "make test*", "cp a ../b"):
        argv = tamper(invocation.argv, "--allowedTools", f"Bash({forged})")
        with pytest.raises(UnsupportedCapability):
            CLAUDE.assert_safe(replace(invocation, argv=argv, allowed_commands=(forged,)), "write_in_repo")
    with pytest.raises(UnsafeInvocationError, match="qualified version"):
        CLAUDE.assert_safe(replace(invocation, allowed_commands_version=None), "write_in_repo")
    with pytest.raises(UnsafeInvocationError, match="tuple"):
        CLAUDE.assert_safe(replace(invocation, allowed_commands=["make test:*"]), "write_in_repo")  # type: ignore[arg-type]


def test_publish_ordering_is_enforced(repo):
    invocation = build(repo, "publish", ("make test:*",))
    reordered = tamper(invocation.argv, "--allowedTools", f"Bash(make test:*),{PUBLISH_ENTRIES}")
    with pytest.raises(UnsafeInvocationError, match="--allowedTools was"):
        CLAUDE.assert_safe(replace(invocation, argv=reordered), "publish")
    with pytest.raises(UnsafeInvocationError, match="--allowedTools was"):
        CLAUDE.assert_safe(replace(invocation, argv=tamper(invocation.argv, "--allowedTools", PUBLISH_ENTRIES)), "publish")


def test_git_is_never_grantable_even_by_forging_the_invocation(repo):
    """The access level's deny still wins on the CLI (measured); here the guard is that no path builds the pattern at all."""
    base = CLAUDE.build_start_argv("x", repo=repo, freedom="write_in_repo", session_id=SESSION, model=None, max_turns=None, reasoning_effort=None)
    with pytest.raises(CommandGrantError, match="git is refused"):
        CLAUDE.configure_allowed_commands(base, ("git commit:*",), "write_in_repo", repo, "2.1.310")


# --- disclosure -----------------------------------------------------------------------------------

@pytest.mark.parametrize("freedom", ["write_in_repo", "publish"])
def test_enforcement_never_overclaims_with_grants(freedom):
    plain = CLAUDE.enforcement(freedom)
    granted = CLAUDE.allowed_commands_enforcement(plain, ("make test:*",))
    assert granted.extra_allowed_commands == ("make test:*",)
    for name in backends.POLICY_FIELDS:
        assert getattr(granted, name) == getattr(plain, name), name
    assert granted.os_enforced is False and granted.commit_push_blocked is False
    assert granted.direct_commit_commands_denied is (freedom == "write_in_repo")
    assert set(plain.caveats) < set(granted.caveats)
    text = " ".join(granted.caveats)
    for needle in ("prefix approvals", "not OS enforcement", "run code from the repository", "chained command needs every part allowed", "can still invoke git", "user and project settings"):
        assert needle in text
    assert "cannot lead" not in text and "cannot re-enable" not in text
    assert "make test:*" not in plain.mechanism and "1 dispatch-time Bash allow pattern" in granted.mechanism


def test_enforcement_omits_the_field_when_empty_and_lists_it_otherwise():
    plain = CLAUDE.enforcement("write_in_repo")
    assert "extra_allowed_commands" not in plain.as_dict()
    assert CLAUDE.allowed_commands_enforcement(plain, ()) is plain
    assert CLAUDE.allowed_commands_enforcement(plain, ("make test",)).as_dict()["extra_allowed_commands"] == ["make test"]
    with pytest.raises(UnsupportedCapability):
        CLAUDE.allowed_commands_enforcement(CLAUDE.enforcement("read_only"), ("make test",))


def test_document_validation_accepts_old_enforcement_and_bounds_the_new_field():
    old = CLAUDE.enforcement("write_in_repo").as_dict()
    qualify_enforcement(old, require_core=True)
    qualify_enforcement({**old, "extra_allowed_commands": ["make test"]}, require_core=True)
    for bad in ("make test", [1], ["make test", "make test"], [f"p{i}" for i in range(17)]):
        with pytest.raises((TaskDocumentError, ValueError)):
            qualify_enforcement({**old, "extra_allowed_commands": bad}, require_core=True)


# --- nested narrowing -----------------------------------------------------------------------------

def parent_enforcement(freedom: str = "write_in_repo", grants: tuple[str, ...] = ()) -> dict:
    return CLAUDE.allowed_commands_enforcement(CLAUDE.enforcement(freedom), grants).as_dict() if freedom in claude_commands.FREEDOMS else CLAUDE.enforcement(freedom).as_dict()


def violation(parent: dict, child: Enforcement):
    return nested_enforcement_violation(parent, child, parent_backend="claude", child_backend="claude", parent_repo="/r", child_repo="/r")


def test_child_grants_must_be_an_exact_subset_of_the_callers():
    parent = parent_enforcement(grants=("make test:*", "cargo build"))
    child = lambda *grants: CLAUDE.allowed_commands_enforcement(CLAUDE.enforcement("write_in_repo"), grants)  # noqa: E731
    assert violation(parent, child("make test:*")) is None
    assert violation(parent, child("make test:*", "cargo build")) is None
    assert violation(parent, child()) is None
    for widened in (("make test",), ("make test:*", "make build"), ("make:*",), ("make test:* ",)):
        found = violation(parent, child(*widened))
        assert found is not None and found[0] == "extra_allowed_commands"


def test_a_caller_without_grants_cannot_grant_and_omission_does_not_inherit():
    parent = parent_enforcement()
    assert violation(parent, CLAUDE.allowed_commands_enforcement(CLAUDE.enforcement("write_in_repo"), ("make test",)))[0] == "extra_allowed_commands"
    assert violation(parent_enforcement(grants=("make test",)), CLAUDE.enforcement("write_in_repo")) is None


def test_an_unrestricted_caller_may_grant_anything():
    parent = CLAUDE.enforcement("unrestricted").as_dict()
    assert violation(parent, CLAUDE.allowed_commands_enforcement(CLAUDE.enforcement("write_in_repo"), ("make test",))) is None


def test_legacy_parent_records_without_the_field_behave_as_no_grants():
    legacy = CLAUDE.enforcement("write_in_repo").as_dict()
    assert "extra_allowed_commands" not in legacy
    assert violation(legacy, CLAUDE.allowed_commands_enforcement(CLAUDE.enforcement("write_in_repo"), ("make test",)))[0] == "extra_allowed_commands"


# --- shared init-version cross-check ----------------------------------------------------------------

GRANTS_Q = {"version": "2.1.310", "observed": "2.1.310", "write": False, "feature": "allowed_commands"}
INIT = {"type": "system", "subtype": "init", "claude_code_version": "2.1.310"}
TOOL = {"type": "assistant", "message": {"content": [{"type": "tool_use", "id": "t", "name": "Bash", "input": {"command": "make test"}}]}}


def test_cross_check_names_the_feature_and_both_versions():
    check_event(INIT, GRANTS_Q)
    check_event(TOOL, GRANTS_Q)
    for event in ({**INIT, "claude_code_version": "2.1.311"}, {k: v for k, v in INIT.items() if k != "claude_code_version"}):
        with pytest.raises(backends.base.ApprovalRefused, match=r"allowed_commands were qualified on 2\.1\.310"):
            check_event(event, GRANTS_Q)
    with pytest.raises(backends.base.ApprovalRefused, match="none before this request"):
        check_event(TOOL, {**GRANTS_Q, "observed": None})
    with pytest.raises(backends.base.ApprovalRefused, match="allowed_commands qualification is missing"):
        check_event(TOOL, {**GRANTS_Q, "version": None})
    with pytest.raises(backends.base.ApprovalRefused, match="caller approvals were qualified"):
        check_event(INIT, {"version": "2.1.302", "observed": None, "write": False})


@pytest.mark.parametrize("pattern", [
    "git fetch origin", "git fetch origin develop", "git pull --ff-only origin develop",
    "git switch develop", "git switch -c feature/63 origin/develop",
    "git checkout -b feature/63 develop", "git -C . pull --ff-only origin develop",
])
@pytest.mark.parametrize("freedom", ["write_in_repo", "publish"])
def test_git_preparation_exact_grants(repo, pattern, freedom):
    invocation = build(repo, freedom, (pattern,))
    if freedom == "write_in_repo":
        assert flag(invocation.argv, "--disallowedTools") == [DISALLOWED_TOOLS]


@pytest.mark.parametrize("pattern", [
    "git fetch:*", "git fetch origin *", "git pull origin develop", "git pull --rebase origin develop",
    "git reset --hard", "git switch -C feature", "git checkout -B feature", "git checkout .",
    "git fetch origin --force", "git fetch https://example.com/repo develop", "git fetch origin +main:main",
    "git -c core.hooksPath=x fetch origin", "git --git-dir=x fetch origin", "git -C escape fetch origin",
    "git -C ../outside fetch origin", "git -C . -C . fetch origin", "git switch -c x HEAD~1",
    "git switch --detach develop", "git branch -D feature", "uv run git fetch origin",
    "git fetch origin refs/../heads", "git push origin develop", "git commit -m x",
])
@pytest.mark.parametrize("freedom", ["write_in_repo", "publish"])
def test_git_preparation_refuses_other_grammar(repo, pattern, freedom):
    with pytest.raises(CommandGrantError):
        claude_commands.validate(pattern, freedom, repo)
