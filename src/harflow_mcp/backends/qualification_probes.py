"""Real, no-paid permission probes, isolated from user config and the external network.

Auto qualification fails closed unless macOS sandbox-exec can confine the harness to
loopback networking and writes in the disposable probe directory. A provider override
alone is not isolation. Only synthetic, bounded actions under that directory are issued.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import selectors
import subprocess
import sys
import time
import uuid
from .qualification import ProbeContext, ProbeResult
from .qualification_provider import final_message, serve

CLAUDE_FEATURES = ('caller_approval_tools', 'caller_write_tools', 'path_safety_prompts', 'denial_texts', 'allowed_commands')
CODEX_FEATURES = ('caller_approval_tools', 'caller_write_tools')
MAX_STREAM = 4 * 1024 * 1024


def _check(condition, reason):
    if not condition:
        raise ValueError(reason)


def _environment(base: Path) -> dict[str, str]:
    # Explicit allowlist: no inherited provider keys, proxies, injected runtime flags,
    # vendor config, shell startup, dispatch context, or dynamic loader environment.
    for name in ('home', 'config', 'codex-home', 'tmp', 'repo', 'outside'):
        (base / name).mkdir(parents=True, exist_ok=True)
    return {'PATH': '/usr/bin:/bin:/usr/sbin:/sbin', 'HOME': str(base / 'home'),
            'CLAUDE_CONFIG_DIR': str(base / 'config'), 'CODEX_HOME': str(base / 'codex-home'),
            'XDG_CONFIG_HOME': str(base / 'home/config'), 'XDG_CACHE_HOME': str(base / 'home/cache'),
            'TMPDIR': str(base / 'tmp'), 'CLAUDE_CODE_TMPDIR': str(base / 'tmp'), 'LANG': 'en_US.UTF-8', 'LC_ALL': 'en_US.UTF-8',
            'SHELL': '/bin/sh', 'CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC': '1',
            'DISABLE_AUTOUPDATER': '1', 'DISABLE_TELEMETRY': '1', 'DISABLE_ERROR_REPORTING': '1'}


def _sandbox(context: ProbeContext, base: Path, env: dict[str, str]) -> list[str]:
    _check(context.identity.platform == 'darwin' and sys.platform == 'darwin', 'no qualified OS isolation adapter for this platform')
    executable = Path(context.identity.executable)
    _check(executable.is_file(), 'probe executable is unavailable')
    digest = hashlib.sha256()
    with executable.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    _check(digest.hexdigest() == context.identity.binary_sha256, 'probe executable identity changed')
    sandbox = '/usr/bin/sandbox-exec'
    _check(Path(sandbox).is_file(), 'sandbox-exec is unavailable; refusing automatic probe')
    # The backend may spawn shells, but their descendants inherit both restrictions.
    profile = '(version 1)(allow default)(deny network*)(deny file-write*)(allow file-write* (subpath ' + json.dumps(str(base)) + ') (literal "/dev/null"))'
    prefix = [sandbox, '-p', profile]
    check = subprocess.run([*prefix, '/usr/bin/true'], cwd=base, env=env, capture_output=True, timeout=5)
    _check(check.returncode == 0, 'OS probe sandbox could not start')
    return prefix


def _provider_prefix(prefix: list[str], url: str) -> list[str]:
    from urllib.parse import urlsplit
    parsed = urlsplit(url)
    _check(parsed.scheme == 'http' and parsed.hostname == '127.0.0.1' and parsed.port is not None,
           'qualification provider must be the local fixture')
    # Permit only this fixture's ephemeral port, never arbitrary local proxies.
    profile = prefix[2] + f'(allow network-outbound (remote ip "localhost:{parsed.port}"))'
    return [prefix[0], prefix[1], profile]


def _drive(argv, *, base, env, initial=b'', on_event, timeout=70):
    """Bounded JSON-line protocol loop; stderr is drained without retaining user data."""
    process = subprocess.Popen(argv, cwd=base / 'repo', env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
    selector = selectors.DefaultSelector()
    buffers = {}
    total, terminal = 0, False
    deadline = time.monotonic() + timeout
    try:
        for stream in (process.stdout, process.stderr):
            selector.register(stream, selectors.EVENT_READ)
            buffers[stream] = bytearray()
        if initial:
            process.stdin.write(initial)
            process.stdin.flush()
        while not terminal:
            _check(time.monotonic() < deadline, 'permission probe timed out')
            ready = selector.select(min(0.25, max(0, deadline - time.monotonic())))
            if not ready and process.poll() is not None:
                break
            for key, _ in ready:
                chunk = os.read(key.fd, 65536)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                total += len(chunk)
                _check(total <= MAX_STREAM, 'probe stream exceeded budget')
                if key.fileobj is process.stderr:
                    continue
                buffer = buffers[key.fileobj]
                buffer.extend(chunk)
                _check(len(buffer) <= MAX_STREAM, 'probe line exceeded budget')
                while b'\n' in buffer:
                    raw, _, rest = buffer.partition(b'\n')
                    buffer[:] = rest
                    if not raw.strip():
                        continue
                    event = json.loads(raw)
                    outgoing, done = on_event(event)
                    for frame in outgoing:
                        process.stdin.write(frame)
                        process.stdin.flush()
                    terminal = terminal or done
            if not selector.get_map():
                break
        _check(terminal, 'harness stopped without a verified terminal result')
    finally:
        selector.close()
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        for stream in (process.stdin, process.stdout, process.stderr):
            stream.close()


def _claude_approvals(context, base, env, prefix, backend, feature):
    """Exercise host decisions and exact grants with the production protocol shapes."""
    repo, outside = base / 'repo', base / 'outside'
    # Safety checks trigger even under acceptEdits. Every tool is exercised both ways.
    trailing = repo / 'trailing '
    trailing.mkdir(exist_ok=True)
    (trailing / 'read.txt').write_text('before')
    (trailing / 'edit.txt').write_text('before')
    (outside / 'read.txt').write_text('outside fixture')
    (outside / 'edit-yes.txt').write_text('before')
    (outside / 'edit-no.txt').write_text('before')
    steps = [
        ('bash-yes', 'Bash', {'command': f'/usr/bin/touch {outside}/bash-yes', 'description': 'qualification'}, True),
        ('bash-no', 'Bash', {'command': f'/usr/bin/touch {outside}/bash-no', 'description': 'qualification'}, False),
        ('read-yes', 'Read', {'file_path': str(trailing / 'read.txt')}, True),
        ('read-no', 'Read', {'file_path': str(outside / 'read.txt')}, False),
        ('safe-write', 'Write', {'file_path': str(trailing / 'new.txt'), 'content': 'safe'}, True),
        ('safe-edit-read', 'Read', {'file_path': str(trailing / 'edit.txt')}, True),
        ('safe-edit', 'Edit', {'file_path': str(trailing / 'edit.txt'), 'old_string': 'before', 'new_string': 'after', 'replace_all': False}, True),
        ('write-yes', 'Write', {'file_path': str(outside / 'write-yes.txt'), 'content': 'written'}, True),
        ('write-no', 'Write', {'file_path': str(outside / 'write-no.txt'), 'content': 'never'}, False),
        ('edit-read-yes', 'Read', {'file_path': str(outside / 'edit-yes.txt')}, True),
        ('edit-yes', 'Edit', {'file_path': str(outside / 'edit-yes.txt'), 'old_string': 'before', 'new_string': 'after', 'replace_all': False}, True),
        ('edit-read-no', 'Read', {'file_path': str(outside / 'edit-no.txt')}, True),
        ('edit-no', 'Edit', {'file_path': str(outside / 'edit-no.txt'), 'old_string': 'before', 'new_string': 'after', 'replace_all': False}, False),
    ]
    selected = {
        'caller_approval_tools': {'bash-yes', 'bash-no', 'read-yes', 'read-no'},
        'caller_write_tools': {'write-yes', 'write-no', 'edit-read-yes', 'edit-yes', 'edit-read-no', 'edit-no'},
        'path_safety_prompts': {'read-yes', 'safe-write', 'safe-edit-read', 'safe-edit'},
    }[feature]
    steps = [step for step in steps if step[0] in selected]
    results, controls, reasons = {}, {}, {}

    def provider(body, _number):
        answered = [block for message in body.get('messages', []) for block in (message.get('content') if isinstance(message.get('content'), list) else []) if isinstance(block, dict) and block.get('type') == 'tool_result']
        for block in answered:
            results[block['tool_use_id']] = block
        index = len(answered)
        if index >= len(steps):
            return [{'type': 'text', 'text': 'qualification complete'}], 'end_turn'
        identifier, name, arguments, _approve = steps[index]
        return [{'type': 'tool_use', 'id': identifier, 'name': name, 'input': arguments}], 'tool_use'

    def event_handler(event):
        if event.get('type') == 'system' and event.get('subtype') == 'init':
            _check(event.get('claude_code_version') == context.identity.version, 'Claude init version changed')
            controls['init'] = True
        if event.get('type') == 'control_request':
            _check(controls.get('init'), 'Claude requested a tool before version initialization')
            request = event.get('request', {})
            identifier = request.get('tool_use_id')
            expected = next((step for step in steps if step[0] == identifier), None)
            _check(expected is not None and identifier not in controls, 'uncorrelated or repeated Claude approval')
            _check(request.get('subtype') == 'can_use_tool' and request.get('tool_name') == expected[1] and request.get('input') == expected[2], f'Claude approval action changed for {identifier}')
            controls[identifier] = True
            reasons[identifier] = request.get('decision_reason_type')
            response = {'behavior': 'allow', 'updatedInput': expected[2]} if expected[3] else {'behavior': 'deny', 'message': 'Caller rejected this exact action'}
            frame = {'type': 'control_response', 'response': {'subtype': 'success', 'request_id': event['request_id'], 'response': response}}
            return [(json.dumps(frame) + '\n').encode()], False
        if event.get('type') == 'result':
            _check(event.get('subtype') == 'success' and not event.get('is_error'), 'Claude probe result failed')
            return [], True
        return [], False

    with serve('anthropic', provider) as (url, requests, errors):
        env.update(ANTHROPIC_API_KEY='local-qualification-no-provider-access', ANTHROPIC_BASE_URL=url)
        invocation = backend.build_start_argv('Perform only scripted qualification actions.', repo=repo, freedom='write_in_repo', session_id=str(uuid.uuid4()), model=None, max_turns=None, reasoning_effort=None)
        args = [context.identity.executable, *invocation.argv[1:], '--permission-prompt-tool', 'stdio', '--permission-prompts', 'host']
        _drive([*_provider_prefix(prefix, url), *args], base=base, env=env, initial=invocation.initial_input, on_event=event_handler)
        _check(requests and not errors, 'fake provider failed')
    features, failures = {}, {}
    def actions(identifiers):
        for identifier, _tool, _input, approve in steps:
            if identifier not in identifiers:
                continue
            _check(controls.get(identifier), f'{identifier} did not reach caller approval')
            _check(identifier in results and bool(results[identifier].get('is_error')) != approve, f'{identifier} did not respect caller decision')
    def command_read():
        actions({'bash-yes', 'bash-no', 'read-yes', 'read-no'})
        _check((outside / 'bash-yes').exists() and not (outside / 'bash-no').exists(), 'command decision escaped')
    def writes():
        actions({'write-yes', 'write-no', 'edit-read-yes', 'edit-yes', 'edit-read-no', 'edit-no'})
        _check((outside / 'write-yes.txt').read_text() == 'written' and not (outside / 'write-no.txt').exists(), 'write decision escaped')
        _check((outside / 'edit-yes.txt').read_text() == 'after' and (outside / 'edit-no.txt').read_text() == 'before', 'edit decision escaped')
    def path_safety():
        actions({'read-yes', 'safe-write', 'safe-edit-read', 'safe-edit'})
        _check(reasons.get('read-yes') == 'other', 'read path safety prompt changed')
        _check(reasons.get('safe-write') == 'safetyCheck' and reasons.get('safe-edit') == 'safetyCheck', 'write/edit path safety prompt changed')
        _check((trailing / 'new.txt').read_text() == 'safe' and (trailing / 'edit.txt').read_text() == 'after', 'path-safety approved write/edit did not run')
    verify, fact = {'caller_approval_tools': (command_read, ['Bash', 'Read']),
                    'caller_write_tools': (writes, ['Write', 'Edit']),
                    'path_safety_prompts': (path_safety, True)}[feature]
    _establish(features, failures, feature, verify, fact)
    return features, failures


def _establish(features, failures, feature, verify, fact, metadata=None):
    """Record only this feature's independently established facts."""
    try:
        verify()
        features[feature] = {feature: fact, **(metadata or {})}
    except Exception as exc:
        failures[feature] = f'{type(exc).__name__}: {str(exc)[:300]}'


def _bundle_result(context, features, failures, reason):
    ok = context.feature in features
    return ProbeResult(ok, reason if ok else failures.get(context.feature, 'feature not established'),
                       facts=features.get(context.feature, {}), features=features, failures=failures)


def _claude(context, base, env, prefix):
    from .claude import ClaudeBackend
    from .claude_denials import match
    backend = ClaudeBackend()
    features, failures = {}, {}
    groups = [((feature,), lambda feature=feature: _claude_approvals(context, base, env, prefix, backend, feature))
              for feature in ('caller_approval_tools', 'caller_write_tools', 'path_safety_prompts')]
    groups.append((('allowed_commands', 'denial_texts'),
                   lambda: _claude_grants(context, base, env, prefix, backend, match)))
    for names, probe in groups:
        try:
            successes, refused = probe()
            features.update(successes)
            failures.update(refused)
        except Exception as exc:
            failures.update({name: f'{type(exc).__name__}: {str(exc)[:300]}' for name in names})
    return _bundle_result(context, features, failures, 'Real isolated CLI independently established the requested permission feature.')


def _claude_grants(context, base, env, prefix, backend, match):
    # Fixed local commands: an allow, an unlisted command, deny precedence, and chaining.
    repo = base / 'repo'
    def git(*args):
        subprocess.run(['/usr/bin/git', '-c', 'core.hooksPath=/dev/null', *args], cwd=repo, env=env, check=True, capture_output=True, timeout=5)
    git('init', '-b', 'develop')
    git('-c', 'user.name=Qualification', '-c', 'user.email=qualification@example.invalid', 'commit', '--allow-empty', '-m', 'fixture')
    git('clone', '--bare', str(repo), str(base / 'remote.git'))
    git('remote', 'add', 'origin', str(base / 'remote.git'))
    steps = [('fetch', 'git -C . fetch origin develop'), ('pull', 'git -C . pull --ff-only origin develop'), ('switch', 'git -C . switch -c qualification/prep'), ('reset', 'git -C . reset --hard'), ('force', 'git -C . pull --ff-only origin develop --force'), ('allowed', 'python3 -m this'), ('unlisted', 'python3 -m calendar'), ('denied', 'git commit -m qualification'), ('chained', 'python3 -m this && python3 -m calendar')]
    results, initialized = {}, []
    def provider(body, _number):
        answered = [block for message in body.get('messages', []) for block in (message.get('content') if isinstance(message.get('content'), list) else []) if isinstance(block, dict) and block.get('type') == 'tool_result']
        for block in answered:
            results[block['tool_use_id']] = block
        if len(answered) >= len(steps):
            return [{'type': 'text', 'text': 'qualification complete'}], 'end_turn'
        identifier, command = steps[len(answered)]
        return [{'type': 'tool_use', 'id': identifier, 'name': 'Bash', 'input': {'command': command, 'description': 'qualification'}}], 'tool_use'
    def event_handler(event):
        if event.get('type') == 'system' and event.get('subtype') == 'init':
            _check(event.get('claude_code_version') == context.identity.version, 'Claude grant init version changed')
            initialized.append(True)
        if event.get('type') == 'result':
            _check(event.get('subtype') == 'success' and not event.get('is_error'), 'Claude probe result failed')
            return [], True
        return [], False
    with serve('anthropic', provider) as (url, requests, errors):
        env['ANTHROPIC_BASE_URL'] = url
        invocation = backend.build_start_argv('Perform scripted grants qualification.', repo=base / 'repo', freedom='write_in_repo', session_id=str(uuid.uuid4()), model=None, max_turns=None, reasoning_effort=None)
        args = [context.identity.executable, *invocation.argv[1:], '--allowedTools', 'Bash(python3 -m this),Bash(git commit:*),Bash(git -C . fetch origin develop),Bash(git -C . pull --ff-only origin develop),Bash(git -C . switch -c qualification/prep)']
        _drive([*_provider_prefix(prefix, url), *args], base=base, env=env, initial=invocation.initial_input, on_event=event_handler)
        _check(requests and not errors and initialized, 'command grants provider/init failed')
    features, failures = {}, {}
    denied = [('unlisted', 'approval_required'), ('denied', 'deny_rule'), ('chained', 'chained_command'), ('reset', 'approval_required'), ('force', 'approval_required')]
    def grant_behavior():
        _check(set(results) == {step[0] for step in steps}, 'grant script incomplete')
        for identifier in ('allowed', 'fetch', 'pull', 'switch'):
            _check(not results[identifier].get('is_error'), f'{identifier} exact allowed command did not execute')
        _check((repo / '.git/HEAD').read_text().strip() == 'ref: refs/heads/qualification/prep', 'branch preparation did not execute')
        for identifier, _expected in denied:
            _check(results[identifier].get('is_error'), f'{identifier} grant escaped')
    def denial_texts():
        for identifier, expected in denied:
            block = results.get(identifier, {})
            _check(block.get('is_error'), f'{identifier} did not produce a denial')
            content = block.get('content')
            text = content if isinstance(content, str) else ''.join(part.get('text', '') for part in content or [] if isinstance(part, dict))
            category, _part = match(text) or (None, None)
            _check(category == expected, f'{identifier} denial text no longer classifies')
    _establish(features, failures, 'allowed_commands', grant_behavior, True)
    _establish(features, failures, 'denial_texts', denial_texts, True)
    return features, failures


def claude_probe(context: ProbeContext) -> ProbeResult:
    try:
        _check(context.feature in CLAUDE_FEATURES, 'unsupported Claude qualification feature')
        base = Path(context.scratch).resolve(strict=True)
        env = _environment(base)
        prefix = _sandbox(context, base, env)
        return _claude(context, base, env, prefix)
    except Exception as exc:
        return ProbeResult(False, f'Claude isolated permission probe refused: {type(exc).__name__}: {str(exc)[:300]}')


def codex_probe(context: ProbeContext) -> ProbeResult:
    try:
        _check(context.feature in CODEX_FEATURES, 'unsupported Codex qualification feature')
        base = Path(context.scratch).resolve(strict=True)
        env = _environment(base)
        prefix = _sandbox(context, base, env)
        return _codex(context, base, env, prefix)
    except Exception as exc:
        return ProbeResult(False, f'Codex isolated permission probe refused: {type(exc).__name__}: {str(exc)[:300]}')


def _codex_group(context, base, env, prefix, feature):
    from . import codex_app_server as protocol
    from .base import Accumulator
    repo, outside = base / 'repo', base / 'outside'
    subprocess.run(['/usr/bin/git', '-c', 'core.hooksPath=/dev/null', 'init', '-q', str(repo)], check=True, env=env, capture_output=True, timeout=5)
    script = []
    probes = [('cmd-yes', 'command'), ('cmd-no', 'command')] if feature == 'caller_approval_tools' else [('patch-yes', 'patch'), ('patch-no', 'patch')]
    for identifier, name in probes:
        target = outside / identifier
        if name == 'command':
            script.append({'type': 'function_call', 'name': 'exec_command', 'call_id': identifier,
                           'arguments': json.dumps({'cmd': '/usr/bin/touch ' + str(target), 'yield_time_ms': 1000, 'sandbox_permissions': 'require_escalated', 'justification': 'qualification'}), 'status': 'completed'})
        else:
            script.append({'type': 'custom_tool_call', 'name': 'apply_patch', 'call_id': identifier,
                           'input': f'*** Begin Patch\n*** Add File: {target}\n+qualification\n*** End Patch', 'status': 'completed'})
    script.append(final_message())
    controls, outcomes = {}, {}
    def provider(_body, number):
        return script[number - 1] if number <= len(script) else final_message()
    with serve('responses', provider) as (url, requests, errors):
        env['OPENAI_API_KEY'] = 'local-qualification-no-provider-access'
        (base / 'codex-home/config.toml').write_text('model_provider="qualification"\nmodel="gpt-5.5"\n[model_providers.qualification]\nname="qualification"\nwire_api="responses"\nrequires_openai_auth=false\nbase_url=' + json.dumps(url + '/v1') + '\n')
        ctx = {'transport': protocol.TRANSPORT, 'version': context.identity.version, 'cwd': str(repo), 'git_root': str(repo), 'freedom': 'write_in_repo', 'network': False, 'prompt': 'Perform only scripted qualification actions.', 'model': 'gpt-5.5', 'write': feature == 'caller_write_tools'}
        acc = Accumulator()
        acc.stream_state['control_context'] = ctx
        initial = b''.join(protocol.initial_frames(acc))
        state = protocol.state_for(acc)
        def event_handler(event):
            if protocol.frame_kind(event) == 'request':
                decoded = protocol.decode(event, {'state': state})
                _check(not decoded.get('system_rejection'), 'Codex approval shape refused by current adapter')
                identifier = decoded['tool_use_id']
                _check(identifier not in controls, 'duplicate Codex approval')
                if decoded['action']['tool_name'] == 'commandExecution':
                    command = decoded['action']['input']['command']
                    identifier = next((name for name in ('cmd-yes', 'cmd-no') if str(outside / name) in command), '')
                else:
                    paths = protocol.change_paths(decoded['action'])
                    identifier = next((name for name in ('patch-yes', 'patch-no') if paths == [str(outside / name)]), '')
                _check(identifier and identifier not in controls, 'uncorrelated Codex approval action')
                approve = identifier.endswith('yes')
                controls[identifier] = decoded['tool_use_id']
                return [protocol.encode(decoded, 'approve' if approve else 'reject')], False
            protocol.step(state, event)
            outcomes.update(protocol.outcomes(event, state))
            return protocol.turn_frames(acc), protocol.finished(acc)
        args = protocol.argv(ctx)
        args[0] = context.identity.executable
        _drive([*_provider_prefix(prefix, url), *args], base=base, env=env, initial=initial, on_event=event_handler)
        _check(requests and not errors and state.terminal and state.terminal['status'] == 'completed', 'Codex fake provider/turn incomplete')
    _check(set(controls) == {name for name, _kind in probes}, 'Codex did not request all approvals')
    for identifier, item in controls.items():
        approve = identifier.endswith('yes')
        _check((outside / identifier).exists() is approve, f'{identifier} ignored caller decision')
        _check(outcomes.get(item) == ('succeeded' if approve else 'failed'), f'{identifier} completion does not match its decision')
    facts = {feature: ['commandExecution'] if feature == 'caller_approval_tools' else ['fileChange'],
             'platforms': ['darwin'], 'freedoms': ['write_in_repo', 'publish'],
             'facts': {'thread_response_reports_effective_policy': True, 'argv_hooks_pin_reflected_by_hooks_list': True, 'decline_accepted_though_unlisted': True}}
    if feature == 'caller_write_tools':
        facts['facts'].update(file_change_request_joins_started_item=True, file_change_accept_does_not_grant_root=True)
    return facts


def _codex(context, base, env, prefix):
    features, failures = {}, {}
    for feature in CODEX_FEATURES:
        try:
            features[feature] = _codex_group(context, base, env, prefix, feature)
        except Exception as exc:
            failures[feature] = f'{type(exc).__name__}: {str(exc)[:300]}'
    return _bundle_result(context, features, failures, 'Real isolated app-server independently established the requested approval feature.')
