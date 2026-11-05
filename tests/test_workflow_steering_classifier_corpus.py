"""ST01 normative final-text outcomes, authored independently of the classifier."""
import json

import pytest

from harflow_mcp.steering_output import classify_final


def frame(body='{"role":"builder","command":{"definition":{}}}'):
    return 'HARFLOW_COMMIT_V1\n' + body + '\nEND_HARFLOW_COMMIT_V1'


@pytest.mark.parametrize('text', [
    'Let us divide the configured work differently.',
    'The old decision_id was dec1. Shall we discuss that?',
    'For example:\n```json\n{"decision_id":"dec1","action":"continue"}\n```',
    'A quoted frame:\n> HARFLOW_COMMIT_V1\n> {"command":{}}\n> END_HARFLOW_COMMIT_V1',
    'Example:\n~~~json\n{"definition":{}}\n~~~',
    'Example:\n````text\n```\nHARFLOW_COMMIT_V1\n```\n````',
    'The marker `HARFLOW_COMMIT_V1` is a quoted example.',
    'Example:\n    HARFLOW_COMMIT_V1\n    {}\n    END_HARFLOW_COMMIT_V1',
    '{"ordinary":"example"}',
    '````\nordinary text\n````',
    'résumé 🛠\r\n  exact prose  ',
])
def test_discussion_corpus_never_commits(text):
    assert classify_final(text)['kind'] == 'conversation'


@pytest.mark.parametrize('text,code', [
    ('', 'empty_output'),
    (' \t\r\n', 'empty_output'),
    ('{"decision_id":"dec1","action":"continue"}', 'commit_frame_required'),
    ('{"status":"succeeded","result":{},"evidence":[]}', 'commit_frame_required'),
    ('```json\n{"definition":{}}\n```', 'commit_frame_required'),
    ('    {"definition":{}}', 'commit_frame_required'),
    ('{"decision_id":', 'malformed_command'),
    ('[', 'malformed_command'),
    ('HARFLOW_COMMIT_V1', 'malformed_command'),
    ('HARFLOW_COMMIT_V', 'malformed_command'),
    ('END_HARFLOW_COMMIT_V1', 'malformed_command'),
    ('Some explanation\n' + frame(), 'malformed_command'),
    (frame() + '\nSome explanation', 'malformed_command'),
    ('```text\n' + frame() + '\n```', 'commit_frame_required'),
    ('~~~text\n' + frame() + '\n~~~', 'commit_frame_required'),
    (frame('{"x":1,"x":2}'), 'malformed_command'),
    (frame('{"x":NaN}'), 'malformed_command'),
    (frame('{} {}'), 'malformed_command'),
    (frame('[]'), 'malformed_command'),
    (frame() + '\n' + frame(), 'conflicting_command'),
    ('Example:\n```\n{"definition":{}}', 'ambiguous_output'),
    ('` HARFLOW_COMMIT_V1', 'ambiguous_output'),
    ('  HARFLOW_COMMIT_V1\n{}\nEND_HARFLOW_COMMIT_V1', 'malformed_command'),
    ('\tHARFLOW_COMMIT_V1\n\t{}\n\tEND_HARFLOW_COMMIT_V1', 'commit_frame_required'),
])
def test_attempted_command_corpus_never_downgrades(text, code):
    assert classify_final(text) == {'kind': 'refused', 'code': code}


@pytest.mark.parametrize('ending', ['\n', '\r\n'])
def test_exact_full_document_frame(ending):
    text = frame().replace('\n', ending)
    outcome = classify_final(text)
    assert outcome['kind'] == 'command'
    assert outcome['command'] == {'role': 'builder', 'command': {'definition': {}}}


def test_non_ascii_whitespace_cannot_hide_command_intent():
    # Non-ASCII whitespace is not allowed around a frame; it cannot authorize execution.
    assert classify_final('\u00a0' + frame())['kind'] != 'command'


def test_json_numeric_overflow_is_refused():
    assert classify_final(frame('{"value":1e999}')) == {'kind': 'refused', 'code': 'malformed_command'}
