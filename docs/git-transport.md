# Git transport in headless runs

Headless runs on every backend receive process-local Git configuration that rewrites
`git@github.com:` and `ssh://git@ssh.github.com:443/` prefixes to `https://github.com/`.
This covers Git subprocesses launched by dependency managers that preserve their inherited environment.
Harflow appends two `GIT_CONFIG_KEY_n` / `GIT_CONFIG_VALUE_n` entries and updates `GIT_CONFIG_COUNT`,
preserving existing entries. Invalid, incomplete, or more than 1024 inherited entries are refused
without printing their contents. No repository config, global Git config, or interactive shell is changed.
Git's own URL matching and higher priority command-line configuration still apply.

The server default is enabled. Set `HARFLOW_GIT_HTTPS_REWRITE=0` (or `false`, `no`, `off`) before
starting the server to disable it. `start_task`, `resume_task`, `continue_task` and
`preview_task_permissions` accept an optional `git_https_rewrite` boolean. Resumes and fresh continuations
inherit the source task's saved effective value unless explicitly overridden; old records fall back
to the server default. The CLI equivalents are `--git-https-rewrite` / `--no-git-https-rewrite` on
`run`, `resume`, `continue` and `permission-preview`. Saved workflow agent candidates accept the same
boolean; explicit native execution cannot accept a per-child environment override and refuses it.
Native children otherwise inherit their existing host environment, outside this headless launch path.

For private GitHub repositories, run `gh auth login` and `gh auth setup-git` interactively. Harflow
never configures credentials, reads tokens, invokes the credential helper, or retains `gh auth status`
output. At stdio server startup it observes `gh auth status --hostname github.com`, then checks whether
Git's URL-matched credential helper value declares `gh auth git-credential`. This observes
a declaration only, not effective helper availability: later generic helper resets and composed
helper chains can change what Git actually executes. A declaration may also exist without being
selected by this query. These subprocesses
have two-second timeouts. The observation is cached for the process lifetime; restart the server after
changing authentication. Embedded/CLI launch owners obtain the same cached observation off the event
loop on first use. Supervised launch owners are separate processes and make their own observation.
The helper check uses the observing server's working directory and environment, not each target
repository's configuration. It does not certify effective helper availability or credential access.

`list_backends` reports `git_environment`; each task's `enforcement.git_environment` and notices
report the effective rewrite and `https_auth_observation`. Possible observations are `gh_missing`,
`gh_not_authenticated`, `gh_authenticated_helper_not_observed`,
`gh_authenticated_helper_declaration_observed`, and `unknown`. While the rewrite is on, observations
that have a fix also carry additive `remedy` (and the same instruction in the task notice):

- `gh_missing`: install GitHub CLI, run `gh auth login` and `gh auth setup-git`, then restart the Harflow server.
- `gh_not_authenticated`: run `gh auth login` and `gh auth setup-git`, then restart the server.
- `gh_authenticated_helper_not_observed`: run `gh auth setup-git`, then restart the server. The observation is cached for the server's lifetime.
- `unknown` and `gh_authenticated_helper_declaration_observed`: no remedy.

The notice also says that, while the rewrite is on, SSH URLs for github.com are fetched over HTTPS, so SSH agent
setup does not affect them, and that `git_https_rewrite=false` restores SSH. With the rewrite off, no HTTPS remedy
is shown. `reachability_verified` is always false.
Authentication status and a helper declaration do not establish sandbox access to a keychain or
helper binary, network access, repository permissions, or successful cloning. Public repositories
may clone without authentication. Harflow preserves the inherited `SSH_AUTH_SOCK`; it does not
create an SSH agent, grant socket access through a backend sandbox, or promise SSH works.

No dependency cache is cleared for automated verification. Unit tests exercise actual Git URL
resolution offline and child-process environment propagation through the common spawn path for
all backend identities. Real dependency installation under each vendor's sandbox remains an
integration check requiring the relevant environment and credentials.
