from dataclasses import replace
from pathlib import Path
import pytest
from polybridge.backends.claude import ClaudeBackend, UnsafeInvocationError


def invocation(prompt, resume=False):
    backend=ClaudeBackend()
    builder=backend.build_resume_argv if resume else backend.build_start_argv
    return builder(prompt,repo=Path('/tmp'),freedom='read_only',session_id='chosen-session',model=None,max_turns=3,reasoning_effort=None)


@pytest.mark.parametrize('resume',[False,True])
def test_capped_large_assignment_uses_complete_one_shot_text_stdin(resume):
    prompt='--dangerously-skip-permissions\r\n'+'x'*1048576+'\nEND雪'
    built=invocation(prompt,resume)
    assert built.stdin_mode=='pipe_once'
    assert built.initial_input==prompt.encode('utf-8')
    assert built.live_input is False
    assert max(len(arg) for arg in built.argv)<100
    assert built.argv[built.argv.index('--max-turns')+1]=='3'
    assert built.argv[built.argv.index('--input-format')+1]=='text'
    assert built.argv[built.argv.index('--resume' if resume else '--session-id')+1]=='chosen-session'
    ClaudeBackend().assert_safe(built,'read_only')


@pytest.mark.parametrize('change',['mode','empty','invalid_utf8','stream','cap','positional','session','whitespace','zero_cap'])
def test_capped_stdin_rejects_mixed_transport_or_session_shapes(change):
    built=invocation('x'*50000)
    argv=list(built.argv)
    if change=='mode':built=replace(built,stdin_mode='pipe')
    elif change=='empty':built=replace(built,initial_input=b'')
    elif change=='invalid_utf8':built=replace(built,initial_input=b'\xff')
    elif change=='whitespace':built=replace(built,initial_input=b' \n\t')
    elif change=='zero_cap':argv[argv.index('--max-turns')+1]='0'
    elif change=='stream':argv[argv.index('--input-format')+1]='stream-json'
    elif change=='cap':a=argv.index('--max-turns');del argv[a:a+2]
    elif change=='positional':argv+=['--','ignored prompt']
    else:argv+=['--resume','other-session']
    built=replace(built,argv=argv)
    with pytest.raises(UnsafeInvocationError):ClaudeBackend().assert_safe(built,'read_only')


def test_small_capped_assignment_retains_measured_classic_shape():
    built=invocation('--dangerously-skip-permissions')
    assert built.argv[-2:]==['--','--dangerously-skip-permissions']
    assert built.stdin_mode=='devnull' and built.initial_input is None


def test_manually_crafted_oversized_classic_prompt_is_refused():
    built=invocation('small')
    with pytest.raises(UnsafeInvocationError,match='one-shot text stdin'):
        ClaudeBackend().assert_safe(replace(built,argv=[*built.argv[:-1],'x'*50000]),'read_only')
