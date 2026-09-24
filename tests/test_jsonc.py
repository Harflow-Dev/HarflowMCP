"""The read-only JSONC tokenizer opencode's config is parsed with. Strings are what it must not break."""

from __future__ import annotations

import pytest

from polybridge.clients import jsonc


def test_plain_json_passes_through_unchanged() -> None:
    text = '{"a": [1, 2, {"b": null}], "c": "d"}'

    assert jsonc.strip(text) == text


def test_line_and_block_comments_are_dropped() -> None:
    text = """{
      // a line comment
      "a": 1, /* a block
      comment */ "b": 2
    }"""

    assert jsonc.loads(text) == {"a": 1, "b": 2}


def test_a_comment_at_end_of_file_without_a_newline() -> None:
    assert jsonc.loads('{"a": 1} // done') == {"a": 1}


def test_comment_markers_inside_strings_are_content() -> None:
    text = '{"url": "https://opencode.ai/config.json", "x": "/* not a comment */", "y": "a // b"}'

    assert jsonc.loads(text) == {
        "url": "https://opencode.ai/config.json",
        "x": "/* not a comment */",
        "y": "a // b",
    }


def test_escaped_quotes_do_not_end_a_string() -> None:
    text = r'{"a": "say \"hi\" // still a string", "b": "back\\"} // comment'

    assert jsonc.loads(text) == {"a": 'say "hi" // still a string', "b": "back\\"}


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ('{"a": 1,}', {"a": 1}),
        ("[1, 2,]", [1, 2]),
        ('{"a": [1,],}', {"a": [1]}),
        ('{"a": 1, // trailing\n}', {"a": 1}),
        ('{"a": 1, /* c */ }', {"a": 1}),
        ('{"a": 1,\n\n  }', {"a": 1}),
    ],
)
def test_trailing_commas_are_dropped(text: str, expected: object) -> None:
    assert jsonc.loads(text) == expected


def test_a_comma_before_a_brace_inside_a_string_is_content() -> None:
    assert jsonc.loads('{"a": ",}", "b": ",]"}') == {"a": ",}", "b": ",]"}


def test_separating_commas_are_kept() -> None:
    assert jsonc.loads('{"a": 1, /* x */ "b": 2}') == {"a": 1, "b": 2}


def test_a_block_comment_between_tokens_does_not_join_them() -> None:
    """`1/**/2` must stay two tokens, so `json` rejects it rather than reading `12`."""
    with pytest.raises(ValueError):
        jsonc.loads("[1/**/2]")


def test_a_byte_order_mark_is_ignored() -> None:
    assert jsonc.loads('﻿{"a": 1}') == {"a": 1}


@pytest.mark.parametrize(
    "text", ['{"a": 1 /* never closed', '{"a": "never closed'], ids=["comment", "string"]
)
def test_unterminated_input_is_an_error(text: str) -> None:
    with pytest.raises(ValueError):
        jsonc.strip(text)


def test_a_double_comma_is_still_rejected() -> None:
    """Only a comma *immediately* before a closer is forgiven; the rest is json's to judge."""
    with pytest.raises(ValueError):
        jsonc.loads('{"a": 1,, }')
