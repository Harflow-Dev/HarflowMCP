"""Self-checks for the scripted fake `claude`: argument vector, replay of the recorded bundle, scripted modes, marker parsing.

The fake is installed like the seam installs it (launcher first on `PATH=<install>/bin:/usr/bin:/bin`) and driven as a
subprocess. No engine code is imported; Python's flag tables and argv shapes are read as text or kept as literal test data.
"""
import ast
import json
import os
from pathlib import Path
import re
import shutil
import signal
import time

import pytest

import fakekit
from fakekit import BUNDLE, MARKER, REPO, SESSION, Rec, frame, live, marker, parse_lines

FAKE_SOURCE = (fakekit.SOURCE / 'fake_claude.py').read_text()
CLAUDE_BACKEND = REPO / 'src' / 'harflow_mcp' / 'backends' / 'claude.py'


def table(source, name):
    """A module-level tuple/frozenset literal of flag names, read from source text only."""
    for node in ast.parse(source).body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(t, ast.Name) and t.id == name for t in targets):
                value = node.value
                if isinstance(value, ast.Call):
                    value = value.args[0]
                return set(ast.literal_eval(value))
    raise AssertionError(f'{name} not found')


def kinds(lines):
    return [(v.get('type'), v.get('subtype')) for v in lines]


def run_live(fake, argv, text, *, extra_frames=(), **kwargs):
    stdin = frame(text) + b''.join(frame(t) for t in extra_frames)
    return fake.run(argv, fake.cwd, stdin=stdin, **kwargs)


# --- argument vector -----------------------------------------------------------------------------------------

def test_the_fake_accepts_the_union_of_the_engine_flag_tables_except_the_approval_transport():
    engine_booleans = table(CLAUDE_BACKEND.read_text(), 'BOOLEAN_FLAGS')
    engine_values = table(CLAUDE_BACKEND.read_text(), 'VALUE_FLAGS')
    fake_booleans, fake_values = table(FAKE_SOURCE, 'BOOLEAN_FLAGS'), table(FAKE_SOURCE, 'VALUE_FLAGS')
    assert '--permission-prompt-tool' in engine_values
    assert (engine_booleans | engine_values) - {'--permission-prompt-tool'} <= fake_booleans | fake_values
    assert '--permission-prompt-tool' not in fake_booleans | fake_values
    assert engine_booleans - {'--permission-prompt-tool'} <= fake_booleans and engine_values - {'--permission-prompt-tool'} <= fake_values
    assert '--add-dir' in table(FAKE_SOURCE, 'REPEATABLE_FLAGS')
    assert table(FAKE_SOURCE, 'PERMISSION_MODES') == {'default', 'plan', 'acceptEdits', 'bypassPermissions'}


# Literal argv of the Python engine's invocations (claude.py build_start_argv/build_resume_argv, with_writable_directory,
# with_shared_directories and with_system_guidance, freedom=unrestricted, model=haiku, effort=low, session id SESSION).
# Regenerate by running those builders once and pasting `invocation.argv[1:]`; the test never imports the engine.
GUIDANCE = 'Harflow: folders may be read-only; do not fall back to /tmp. Never change settings to enable writes.'
ENGINE_START_LIVE = ['-p', '--output-format', 'stream-json', '--verbose', '--include-partial-messages', '--permission-mode', 'bypassPermissions',
                     '--model', 'haiku', '--effort', 'low', '--input-format', 'stream-json', '--session-id', SESSION]
ENGINE_START_COMPOSED = [*ENGINE_START_LIVE, '--add-dir', '/private/tmp/scratch-a', '--add-dir', '/private/tmp/sh1', '--add-dir', '/private/tmp/sh2',
                         '--append-system-prompt', GUIDANCE]
ENGINE_RESUME_LIVE = ['-p', '--output-format', 'stream-json', '--verbose', '--include-partial-messages', '--permission-mode', 'bypassPermissions',
                      '--model', 'haiku', '--effort', 'low', '--input-format', 'stream-json', '--resume', SESSION]
ENGINE_START_CAPPED = ['-p', '--output-format', 'stream-json', '--verbose', '--include-partial-messages', '--permission-mode', 'bypassPermissions',
                       '--max-turns', '3', '--model', 'haiku', '--effort', 'low', '--session-id', SESSION, '--', 'hello']
ENGINE_RESUME_CAPPED = ['-p', '--output-format', 'stream-json', '--verbose', '--include-partial-messages', '--permission-mode', 'bypassPermissions',
                        '--max-turns', '3', '--model', 'haiku', '--effort', 'low', '--resume', SESSION, '--', 'hello']
ENGINE_START_TEXT = ['-p', '--output-format', 'stream-json', '--verbose', '--include-partial-messages', '--permission-mode', 'bypassPermissions',
                     '--max-turns', '2', '--model', 'haiku', '--effort', 'low', '--input-format', 'text', '--session-id', SESSION]


@pytest.mark.parametrize('argv,recording', [(ENGINE_START_LIVE, 'r02_live_single'), (ENGINE_START_COMPOSED, 'r02_live_single'),
                                            (ENGINE_RESUME_LIVE, 'r05_resume')])
def test_engine_live_argv_shapes_are_accepted_and_replayed(fake, argv, recording):
    code, lines, err, _ = run_live(fake, argv, 'hello')
    rec = Rec(recording)
    assert code == 0 and err == b''
    assert lines == rec.expect(session=SESSION, cwd=fake.real_cwd, model='haiku')


@pytest.mark.parametrize('argv,recording', [(ENGINE_START_CAPPED, 'r01_oneshot'), (ENGINE_RESUME_CAPPED, 'r05_resume')])
def test_engine_capped_positional_argv_shapes_are_accepted(fake, argv, recording):
    code, lines, _, _ = fake.run(argv, fake.cwd)
    assert code == 0 and lines == Rec(recording).expect(session=SESSION, cwd=fake.real_cwd, model='haiku')


def test_engine_large_prompt_shape_reads_text_stdin_to_eof(fake):
    code, lines, _, _ = fake.run(ENGINE_START_TEXT, fake.cwd, stdin=('x' * 40000).encode())
    assert code == 0 and lines == Rec('r01_oneshot').expect(session=SESSION, cwd=fake.real_cwd, model='haiku')


@pytest.mark.parametrize('argv,reason', [
    ([*live(), '--bogus'], "unknown option '--bogus'"),
    ([*live(session=('--session-id', SESSION)), '--resume', SESSION], '--session-id and --resume conflict'),
    ([*live(), '--permission-prompt-tool', 'mcp__x__y'], 'approval transport is not faked'),
    ([*live(), '--model=haiku'], "unknown option '--model=haiku'"),
    (['--output-format', 'stream-json', '--verbose', '--input-format', 'stream-json', '--session-id', SESSION], 'use -p'),
    (['-p', '--output-format', 'text', '--verbose', '--session-id', SESSION, '--', 'x'], 'only --output-format stream-json'),
    (['-p', '--output-format', 'stream-json', '--session-id', SESSION, '--', 'x'], 'requires --verbose'),
    (['-p', '--output-format', 'stream-json', '--verbose', '--permission-mode', 'dontAsk', '--', 'x'], 'permission-mode'),
    ([*live(), '--model', 'a', '--model', 'b'], 'specified more than once'),
    (['-p', '--output-format', 'stream-json', '--verbose', '--model'], 'argument missing'),
    (['-p', '--output-format', 'stream-json', '--verbose', '--max-turns', '0', '--', 'x'], '--max-turns'),
    (['-p', '--output-format', 'stream-json', '--verbose', '--', 'one', 'two'], 'more than one positional'),
    (['--version', '-p'], 'cannot be combined'),
])
def test_refused_argument_vectors_exit_one_with_one_stderr_line_and_a_refused_ledger(fake, argv, reason):
    code, lines, err, raw = fake.run(argv, fake.cwd)
    assert code == 1 and raw == b'' and err.count(b'\n') == 1 and reason in err.decode()
    [path] = fakekit.ledgers(fake.ledger_dir)
    rows = fakekit.read_rows(path)
    assert rows[0]['event'] == 'start' and rows[0]['kind'] == 'refused' and reason in rows[0]['reason'] and 'ack_token' not in rows[0]
    assert rows[-1] == {**rows[-1], 'event': 'exit', 'code': 1}


def test_repeated_add_dir_is_the_only_repeatable_option(fake):
    code, *_ = run_live(fake, [*live(), '--add-dir', '/a', '--add-dir', '/b'], 'x')
    assert code == 0
    code, _, err, _ = run_live(fake, [*live(), '--effort', 'low', '--effort', 'high'], 'x')
    assert code == 1 and b'more than once' in err


def test_version_probe_prints_the_recorded_line_and_touches_nothing(fake):
    fake.set(ledger_dir=str(fake.ledger_dir / 'never-created'), observer_socket='/nonexistent/o.sock', nonce='0' * 32)
    code, raw, err = fake.run_raw(['--version'], fake.cwd)
    assert code == 0 and err == b'' and raw == (BUNDLE / 'version.txt').read_bytes() == b'2.1.322 (Claude Code)\n'
    assert not (fake.ledger_dir / 'never-created').exists(), 'the fast path must write no ledger'
    # a tampered bundle does not matter to the fast path: no bundle hashing happens
    clone = fake.clone(fake.work / 'clone')
    (clone.fake / 'recordings' / 'r02_live_single' / 'stdout.jsonl').write_bytes(b'tampered\n')
    assert clone.run_raw(['--version'], fake.cwd)[1] == raw


# --- replay of the recorded bundle ---------------------------------------------------------------------------

def test_the_default_mode_replays_the_live_recording_with_documented_substitutions(fake):
    code, lines, err, raw = run_live(fake, live(), 'plain prompt, no marker')
    rec = Rec('r02_live_single')
    assert code == 0 and err == b''
    assert lines == rec.expect(session=SESSION, cwd=fake.real_cwd)
    assert SESSION in raw.decode() and rec.session not in raw.decode() and rec.cwd not in raw.decode()


def test_model_flag_replaces_only_the_init_model(fake):
    _, lines, *_ = run_live(fake, live('--model', 'my-model'), 'x')
    assert lines == Rec('r02_live_single').expect(session=SESSION, cwd=fake.real_cwd, model='my-model')
    assert [v['model'] for v in lines if v.get('subtype') == 'init'] == ['my-model']


def test_without_substitution_inputs_the_bundle_is_replayed_byte_for_byte(fake):
    rec = Rec('r02_live_single')
    # the recorded session id passed back and a cwd equal to the recorded one cannot be forced, so compare line kinds and bytes of
    # lines that carry neither substitution
    _, lines, _, raw = run_live(fake, live(session=('--session-id', rec.session)), 'x')
    assert kinds(lines) == kinds(rec.lines)
    out_lines = [r for r in raw.split(b'\n') if r]
    unchanged = [i for i, (a, b) in enumerate(zip(out_lines, rec.raw)) if a == b]
    assert unchanged, 'lines without a cwd must be replayed unchanged'


@pytest.mark.parametrize('mode,name', [('tool_use', 'r06_tool_use'), ('permission_denied', 'r07_permission_denied'), ('complete', 'r02_live_single')])
def test_recorded_modes_reemit_their_bundle_lines(fake, mode, name):
    code, lines, err, _ = run_live(fake, live(), marker(mode=mode))
    rec = Rec(name)
    assert code == 0 and err == b'' and lines == rec.expect(session=SESSION, cwd=fake.real_cwd)
    assert kinds(lines) == kinds(rec.lines)


def test_resume_replays_the_resume_recording_with_the_resumed_session(fake):
    other = '99999999-8888-4777-8666-555555555555'
    _, lines, *_ = run_live(fake, live(session=('--resume', other)), 'continue')
    rec = Rec('r05_resume')
    assert lines == rec.expect(session=other, cwd=fake.real_cwd)
    assert {v['session_id'] for v in lines if 'session_id' in v} == {other}


def test_error_model_replays_lines_stderr_and_exit_one(fake):
    code, lines, err, _ = run_live(fake, live(), marker(mode='error_model'))
    rec = Rec('r08_error_bad_model')
    assert code == 1 == rec.run['exit_code'] and err == rec.stderr
    assert lines == rec.expect(session=SESSION, cwd=fake.real_cwd)
    assert lines[-1]['type'] == 'result' and lines[-1]['is_error'] is True


def test_live_multiturn_replays_one_turn_per_frame_and_exits_at_eof(fake):
    rec = Rec('r02_live_single')
    code, lines, _, _ = run_live(fake, live(), 'one', extra_frames=['two', 'three'])
    assert code == 0 and len(rec.turns()) == 1
    assert lines == rec.expect(session=SESSION, cwd=fake.real_cwd) * 3
    assert [v['type'] for v in lines].count('result') == 3


def test_multiturn_mode_replays_each_recorded_turn_then_the_last_one_again(fake):
    rec = Rec('r03_live_multiturn')
    turns = rec.turns()
    assert len(turns) == 2
    code, lines, *_ = run_live(fake, live(), marker(mode='multiturn'), extra_frames=['second', 'third'])
    expected = [*rec.expect(*turns[0], session=SESSION, cwd=fake.real_cwd), *rec.expect(*turns[1], session=SESSION, cwd=fake.real_cwd),
                *rec.expect(*turns[1], session=SESSION, cwd=fake.real_cwd)]
    assert lines == expected and code == rec.run['exit_code'] == 1


def test_stdin_closed_before_any_frame_prints_nothing_and_exits_zero(fake):
    code, lines, err, raw = fake.run(live(), fake.cwd)
    assert (code, raw, err) == (0, b'', b'') and Rec('r10_early_stdin_close').run['exit_code'] == 0


def test_non_user_frames_after_the_first_are_ignored_and_a_non_user_first_frame_is_refused(fake):
    code, lines, *_ = fake.run(live(), fake.cwd, stdin=frame('x') + b'{"type":"control"}\n' + frame('y'))
    assert code == 0 and [v['type'] for v in lines].count('result') == 2
    code, lines, err, raw = fake.run(live(), fake.cwd, stdin=b'{"type":"control"}\n')
    assert code == 70 and raw == b'' and b'first input frame is not a user message' in err
    code, _, err, _ = fake.run(live(), fake.cwd, stdin=b'not json\n')
    assert code == 70 and b'not JSON' in err


def test_oneshot_positional_prompt_after_double_dash(fake):
    argv = [*fakekit.COMMON, '--session-id', SESSION, '--', 'Reply with the single word OK.']
    code, lines, _, _ = fake.run(argv, fake.cwd)
    rec = Rec('r01_oneshot')
    assert code == 0 and lines == rec.expect(session=SESSION, cwd=fake.real_cwd)
    # a prompt that looks like an option rides after `--` and is never parsed as one
    code, lines, *_ = fake.run([*fakekit.COMMON, '--session-id', SESSION, '--', '--bogus'], fake.cwd)
    assert code == 0 and lines == rec.expect(session=SESSION, cwd=fake.real_cwd)


def test_oneshot_text_stdin_and_marker_in_either_transport(fake):
    argv = [*fakekit.COMMON, '--input-format', 'text', '--session-id', SESSION]
    code, lines, *_ = fake.run(argv, fake.cwd, stdin=b'whole stdin is the prompt\n')
    assert code == 0 and lines == Rec('r01_oneshot').expect(session=SESSION, cwd=fake.real_cwd)
    code, *_ = fake.run(argv, fake.cwd, stdin=marker(mode='nonzero', exit_code=9).encode())
    assert code == 9
    code, *_ = fake.run([*fakekit.COMMON, '--session-id', SESSION, '--', marker(mode='nonzero')], fake.cwd)
    assert code == 7


def test_sigint_after_init_reproduces_the_interrupt_recording(fake):
    rec = Rec('r09_interrupt')
    proc = fake.spawn(live(), fake.cwd)
    proc.send(frame(marker(mode='interrupt')))
    before = proc.read_lines(2)
    assert before == rec.expect(0, 2, session=SESSION, cwd=fake.real_cwd) and rec.run['signals'] == [[2, 1788]]
    time.sleep(.5)
    assert proc.alive(), 'the turn stays open until the signal'
    proc.p.send_signal(signal.SIGINT)
    code, after, _ = proc.finish(close=False)
    assert code == rec.run['exit_code'] == 0
    assert after == rec.expect(2, None, session=SESSION, cwd=fake.real_cwd)
    assert after[0]['message']['content'][0]['text'] == '[Request interrupted by user]'
    assert after[-1]['type'] == 'result' and after[-1]['subtype'] == 'error_during_execution'
    rows = fakekit.read_rows(fakekit.ledgers(fake.ledger_dir)[0])
    assert {'event': 'signal', 'signum': 2, 'turn_open': True}.items() <= next(r for r in rows if r['event'] == 'signal').items()


def test_sigint_while_a_scripted_turn_is_open_also_emits_the_interrupt_lines(fake):
    gate = fake.work / 'gate'
    proc = fake.spawn(live(), fake.cwd)
    proc.send(frame(marker(mode='hold', gate=str(gate))))
    proc.read_lines(Rec('r02_live_single').first_assistant_split())
    proc.p.send_signal(signal.SIGINT)
    code, after, _ = proc.finish(close=False)
    assert code == 0 and after == Rec('r09_interrupt').expect(2, None, session=SESSION, cwd=fake.real_cwd)


def test_sigint_while_idle_dies_by_the_signal(fake):
    proc = fake.spawn(live(), fake.cwd)
    proc.send(frame('x'))
    proc.read_lines(len(Rec('r02_live_single').lines))
    time.sleep(.3)
    proc.p.send_signal(signal.SIGINT)
    code, rest, _ = proc.finish(close=False)
    assert code == -signal.SIGINT and rest == []


def test_interrupt_mode_stdin_eof_ends_the_pending_turn_quietly(fake):
    proc = fake.spawn(live(), fake.cwd)
    proc.send(frame(marker(mode='interrupt')))
    proc.read_lines(2)
    proc.close_stdin()
    code, rest, _ = proc.finish()
    assert code == 0 and rest == []


# --- scripted modes -------------------------------------------------------------------------------------------

def test_nonzero_replays_the_error_result_and_exits_with_the_requested_code(fake):
    rec = Rec('r08_error_bad_model')
    for spec, expected in (({}, 7), ({'exit_code': 42}, 42), ({'exit_code': 255}, 255)):
        code, lines, err, _ = run_live(fake, live(), marker(mode='nonzero', **spec))
        assert code == expected and lines == rec.expect(session=SESSION, cwd=fake.real_cwd) and err == rec.stderr
    [mode] = [r for r in fakekit.read_rows(fakekit.ledgers(fake.ledger_dir)[0]) if r['event'] == 'mode']
    assert mode['mode'] == 'nonzero' and mode['scripted'] is True


@pytest.mark.parametrize('code', [0, 256, -1, '7', True, 1.5, None])
def test_nonzero_rejects_a_bad_exit_code(fake, code):
    result, _, err, raw = run_live(fake, live(), marker(mode='nonzero', exit_code=code))
    assert result == 70 and raw == b'' and b'exit_code' in err


def test_hold_waits_for_the_gate_then_replays_the_tail_and_exits_zero(fake):
    rec = Rec('r02_live_single')
    split = rec.first_assistant_split()
    gate = fake.work / 'gate'
    proc = fake.spawn(live(), fake.cwd)
    proc.send(frame(marker(mode='hold', gate=str(gate))))
    assert proc.read_lines(split) == rec.expect(0, split, session=SESSION, cwd=fake.real_cwd)
    time.sleep(.5)
    assert proc.alive() and proc._buffer == b''
    gate.write_bytes(b'')
    code, rest, _ = proc.finish(close=False)
    assert code == 0 and rest == rec.expect(split, None, session=SESSION, cwd=fake.real_cwd)


def test_hold_ends_at_stdin_eof_unless_survive_eof(fake):
    gate = fake.work / 'gate'
    split = Rec('r02_live_single').first_assistant_split()
    proc = fake.spawn(live(), fake.cwd)
    proc.send(frame(marker(mode='hold', gate=str(gate))))
    proc.read_lines(split)
    proc.close_stdin()
    code, rest, _ = proc.finish()
    assert code == 0 and rest == [], 'EOF ends a holding fake without more output'

    survivor = fake.spawn(live(), fake.cwd)
    survivor.send(frame(marker(mode='hold', gate=str(gate), survive_eof=True)))
    survivor.read_lines(split)
    survivor.close_stdin()
    time.sleep(1)
    assert survivor.alive(), 'survive_eof ignores stdin EOF'
    gate.write_bytes(b'')
    code, rest, _ = survivor.finish()
    assert code == 0 and rest == Rec('r02_live_single').expect(split, None, session=SESSION, cwd=fake.real_cwd)


def test_sigterm_kills_the_fake_by_signal_and_leaves_an_incomplete_ledger(fake):
    gate = fake.work / 'gate'
    proc = fake.spawn(live(), fake.cwd)
    proc.send(frame(marker(mode='hold', gate=str(gate))))
    proc.read_lines(Rec('r02_live_single').first_assistant_split())
    proc.p.send_signal(signal.SIGTERM)
    code, _, _ = proc.finish(close=False)
    assert code == -signal.SIGTERM
    rows = fakekit.read_rows(fakekit.ledgers(fake.ledger_dir)[0])
    assert rows[0]['event'] == 'start' and not any(r['event'] == 'exit' for r in rows)


def test_stop_file_ends_any_waiting_mode_with_exit_zero_and_no_more_output(fake):
    gate = fake.work / 'gate'
    split = Rec('r02_live_single').first_assistant_split()
    proc = fake.spawn(live(), fake.cwd)
    proc.send(frame(marker(mode='hold', gate=str(gate), survive_eof=True)))
    proc.read_lines(split)
    fake.stop_file.write_bytes(b'')
    code, rest, _ = proc.finish()
    assert code == 0 and rest == []
    exits = [r for r in fakekit.read_rows(fakekit.ledgers(fake.ledger_dir)[0]) if r['event'] == 'exit']
    assert exits and exits[-1]['code'] == 0 and exits[-1]['note'] == 'stop file'
    # an idle live run with the stop file already present also exits quietly
    idle = fake.spawn(live(), fake.cwd)
    code, rest, _ = idle.finish()
    assert code == 0 and rest == []


def test_a_write_error_is_logged_and_exits_74(fake):
    gate = fake.work / 'gate'
    split = Rec('r02_live_single').first_assistant_split()
    proc = fake.spawn(live(), fake.cwd)
    proc.send(frame(marker(mode='hold', gate=str(gate), survive_eof=True)))
    proc.read_lines(split)
    proc.p.stdout.close()  # nobody reads the fake's stdout any more
    gate.write_bytes(b'')
    code = proc.p.wait(timeout=60)
    assert code == 74
    rows = fakekit.read_rows(fakekit.ledgers(fake.ledger_dir)[0])
    failed = [r for r in rows if r['event'] == 'io' and r.get('error')]
    assert failed and failed[0]['direction'] == 'stdout' and failed[0]['written'] < failed[0]['bytes'] and 'errno' in failed[0]['error']
    assert rows[-1]['event'] == 'exit' and rows[-1]['code'] == 74


def test_lifetime_bound_ends_a_forgotten_fake_with_exit_70(fake):
    fake.set(lifetime=3)
    proc = fake.spawn(live(), fake.cwd)
    proc.send(frame(marker(mode='hold', gate=str(fake.work / 'never'), survive_eof=True)))
    proc.read_lines(Rec('r02_live_single').first_assistant_split())
    assert proc.p.wait(timeout=30) == 70


@pytest.mark.parametrize('spec', [{'mode': 'hold'}, {'mode': 'hold', 'gate': 'relative/gate'}, {'mode': 'hold', 'gate': 5},
                                  {'mode': 'hold', 'gate': '/abs', 'survive_eof': 'yes'}, {'mode': 'escape', 'gate': '/abs', 'descendant_holds_pipes': 1}])
def test_scripted_gate_and_flag_validation(fake, spec):
    code, _, err, raw = run_live(fake, live(), marker(**spec))
    assert code == 70 and raw == b'' and err.startswith(b'harflow fake claude: ')


def test_gate_outside_the_case_workspace_refuses(fake, tmp_path):
    outside = tmp_path / 'outside'
    outside.mkdir()
    (fake.work / 'link').symlink_to(outside)
    for gate in (outside / 'gate', fake.work, fake.work / '..' / 'outside' / 'gate', fake.work / 'link' / 'gate', '/etc/hosts'):
        code, _, err, raw = run_live(fake, live(), marker(mode='hold', gate=str(gate)))
        assert code == 70 and b'gate outside the case workspace' in err, gate
        assert raw == b'', 'the gate is validated before any output'
    fake.set(workspace=None)
    code, _, err, _ = run_live(fake, live(), marker(mode='hold', gate=str(fake.work / 'gate')))
    assert code == 70 and b'no workspace configured' in err


# --- marker ---------------------------------------------------------------------------------------------------

def run_blocks(fake, blocks, **kwargs):
    stdin = (json.dumps({'type': 'user', 'message': {'role': 'user', 'content': blocks}}) + '\n').encode()
    return fake.run(live(), fake.cwd, stdin=stdin, **kwargs)


def test_only_the_first_text_block_of_the_first_frame_is_parsed(fake):
    text = lambda t: {'type': 'text', 'text': t}
    code, *_ = run_blocks(fake, [text('plain'), text(marker(mode='nonzero'))])
    assert code == 0
    code, *_ = run_blocks(fake, [{'type': 'image', 'source': {}}, text(marker(mode='nonzero', exit_code=3)), text(marker(mode='nonzero', exit_code=4))])
    assert code == 3
    code, *_ = run_blocks(fake, [text('leading words 日本語\n' + marker(mode='nonzero', exit_code=5) + ' trailing text {"mode": "hold"}')])
    assert code == 5
    # a marker in a later frame never changes the run
    code, lines, *_ = fake.run(live(), fake.cwd, stdin=frame('plain') + frame(marker(mode='nonzero')))
    assert code == 0 and [v['type'] for v in lines].count('result') == 2


def test_string_content_is_accepted_as_the_first_text(fake):
    stdin = (json.dumps({'type': 'user', 'message': {'role': 'user', 'content': marker(mode='nonzero', exit_code=6)}}) + '\n').encode()
    assert fake.run(live(), fake.cwd, stdin=stdin)[0] == 6


@pytest.mark.parametrize('text', [MARKER + ' {nope', MARKER + ' [1]', MARKER, MARKER + ' {"mode": "nope"}', MARKER + ' {"mode": "complete", "x": 1}',
                                  MARKER + ' {"mode": "nonzero", "gate": "/x"}', MARKER + ' {"mode": 5}'])
def test_malformed_markers_exit_70_with_a_ledger_record_and_no_fallback(fake, text):
    code, _, err, raw = run_live(fake, live(), text)
    assert code == 70 and raw == b'' and err.startswith(b'harflow fake claude: ')
    rows = fakekit.read_rows(fakekit.ledgers(fake.ledger_dir)[0])
    assert [r['event'] for r in rows if r['event'] in ('marker-error', 'exit')] == ['marker-error', 'exit']
    assert rows[-1]['code'] == 70


def test_input_bounds_refuse_with_exit_70(fake):
    code, _, err, _ = fake.run(live(), fake.cwd, stdin=b'{"type":"user","pad":"' + b'x' * (65 * 1024) + b'"}\n')
    assert code == 70 and b'input frame exceeded its bound' in err
    padded = (json.dumps({'type': 'user', 'message': {'content': 'p' * 60000}}) + '\n').encode()
    code, _, err, _ = fake.run(live(), fake.cwd, stdin=padded * 20)
    assert code == 70 and b'total bound' in err


# --- bundle integrity -----------------------------------------------------------------------------------------

def tamper_cases():
    def edit(clone):
        (clone.fake / 'recordings' / 'r02_live_single' / 'stdout.jsonl').write_bytes(b'{"type":"result"}\n')

    def extra(clone):
        (clone.fake / 'recordings' / 'extra.txt').write_text('x')

    def delete(clone):
        (clone.fake / 'recordings' / 'r05_resume' / 'stderr.txt').unlink()

    def link(clone):
        (clone.fake / 'recordings' / 'link').symlink_to(clone.fake / 'fake_claude.py')

    def sums(clone):
        path = clone.fake / 'recordings' / 'SHA256SUMS'
        path.write_text(path.read_text().replace('0', '1', 1) if '0' in path.read_text() else path.read_text() + 'zz\n')

    def sums_garbage(clone):
        (clone.fake / 'recordings' / 'SHA256SUMS').write_text('not a digest line\n')

    def traversal(clone):
        (clone.fake / 'recordings' / 'SHA256SUMS').write_text(f'{"0" * 64}  ../fake_claude.py\n')
    return [edit, extra, delete, link, sums, sums_garbage, traversal]


@pytest.mark.parametrize('tamper', tamper_cases(), ids=lambda f: f.__name__)
def test_a_bundle_that_differs_from_its_digest_list_refuses_before_any_output(fake, tmp_path, tamper):
    clone = fake.clone(tmp_path / 'clone')
    clone.ledger_dir = tmp_path / 'clone-ledger'
    clone.set()
    assert run_live(clone, live(), 'x')[0] == 0, 'the untampered clone must work'
    tamper(clone)
    code, _, err, raw = run_live(clone, live(), 'x')
    assert code == 70 and raw == b'' and b'recording bundle' in err
    assert len(fakekit.ledgers(tmp_path / 'clone-ledger')) == 1, 'a refused bundle writes no agent ledger'


def test_the_shipped_bundle_matches_its_own_digest_list():
    import hashlib
    sums = dict(reversed(line.split('  ', 1)) for line in (BUNDLE / 'SHA256SUMS').read_text().splitlines())
    files = {str(p.relative_to(BUNDLE)): hashlib.sha256(p.read_bytes()).hexdigest() for p in BUNDLE.rglob('*')
             if p.is_file() and p.name not in ('SHA256SUMS', '.DS_Store') and '__pycache__' not in p.parts}
    assert sums == files
    assert not any(re.search(r'/Users/|/home/[a-z]', p.read_text(errors='ignore')) for p in BUNDLE.rglob('*') if p.is_file() and p.name != 'SHA256SUMS'
                   and 'README' not in p.name), 'no personal absolute path in the recordings'
