# Git transport in headless runs

By default, headless runs on every backend receive process-local Git configuration that rewrites
`git@github.com:` and `ssh://git@ssh.github.com:443/` prefixes to `https://github.com/`.
The rewrite applies to matching URLs in Git processes that preserve this injected configuration.
Tools that remove or replace it, or use a different SCM implementation, may bypass it. Enabling
the rewrite reports configuration, not proof that a dependency was fetched over HTTPS.
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

With the rewrite off, no HTTPS remedy is shown. Disabling Harflow's rewrite does not force SSH:
other Git configuration may still rewrite URLs. `reachability_verified` is always false.
Authentication status and a helper declaration do not establish sandbox access to a keychain or
helper binary, network access, repository permissions, or successful cloning. Public repositories
may clone without authentication. Harflow preserves the inherited `SSH_AUTH_SOCK`; it does not
create an SSH agent, grant socket access through a backend sandbox, or promise SSH works.

## Dependency managers and diagnosis

The supported route is system Git with the injected `GIT_CONFIG_*` entries preserved through the
tool chain. In isolated macOS tests, SwiftPM 6.3.3 preserved them into Git; Claude Code 2.1.310
Bash also preserved them through SwiftPM. Fresh resolution, a populated-cache update and a
configured mirror selected the rewrite. The same cached update without the rewrite selected SSH.
These tests used synthetic dependencies and did not verify GitHub authentication or network access.

An isolated test with Xcode 26.6 (17F113) used a synthetic localhost SSH dependency and a
command-scope rewrite to a local file. `xcodebuild` with `-scmProvider system` resolved it; with
`-scmProvider xcode` under the same environment, the built-in provider attempted the original SSH
URL and failed. A direct local file dependency succeeded with the built-in provider. This shows
the measured provider bypass for that configuration, not a GitHub or HTTPS authentication test,
and does not establish behavior for every Xcode version. To select the measured system route:

```sh
xcodebuild -resolvePackageDependencies -project Project.xcodeproj -scmProvider system
```

The calling environment must preserve the injected configuration. Check the actual command and
provider selected by any wrapper; Xcode GUI behavior is unqualified. Tuist was unavailable for
testing, and the original invocation chain and cause of the reported SSH failure remain unknown.

When diagnosing a failure, identify the actual command and binary, provider, cache or mirror stored
URLs, and relevant shell settings. Check only the presence and count of selected `GIT_CONFIG_*`
variables; do not dump environment variables, Git configuration, credential helpers or traces that
may contain secrets. Do not clear real dependency caches as a diagnostic shortcut. A `Fetching`
line showing an SSH manifest URL does not prove the effective transport. Likewise, if OpenSSH
maps `Host github.com` to `HostName ssh.github.com` and `Port 443`, an error naming
`ssh.github.com` can reflect that alias rather than a different dependency URL.

Harflow does not install a persistent or task-scoped Git configuration wrapper: there is no
evidence that one repairs a built-in SCM route, and changing configuration precedence, includes,
credential helpers, permissions or lifecycle would introduce separate risks. Unit tests exercise
Git URL resolution offline and child-process environment propagation through the common spawn
path. Real dependency installation under a vendor's sandbox remains an integration check requiring
the relevant environment and credentials.
