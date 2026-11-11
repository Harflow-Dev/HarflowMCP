"""E1 case for the `data.reason` of `task-artifacts` cursor errors (`task-artifacts.v1`).

Precedence (`docs/contracts.md`, artifact error reasons): a limit error first (no reason), then the cursor's shape
(`cursor_invalid`), then its binding to the owner and manifest revision (`cursor_stale`, whatever the offset), then
its offset range (`cursor_invalid`). `ownership_preparing` is not produced by these reads at E1: no workflow
catalog exists to prepare, so no case expects it.
"""
import base64
import json

from .cases import CORE, case, error, success
from .world import A, C, MISSING, artifact

ART = ('task-artifacts.v1',)
ART1, ART2 = '11111111-2222-4333-8444-555555555555', '11111111-2222-4333-8444-666666666666'


def reason(ctx, res, expected, label):
    err = error(ctx, res, 'artifact_error', label=label)
    data = err.get('data')
    got = data.get('reason', '<absent>') if isinstance(data, dict) else '<absent>'
    ctx.eq(got, expected, f'{label}: error.data.reason [contract]')


def encode(value):
    return base64.urlsafe_b64encode(json.dumps(value).encode()).decode()


def decode(ctx, cursor):
    """The opaque cursor (a base64 JSON [owner, revision, offset]) as a list, or None after one named skip."""
    try:
        value = json.loads(base64.urlsafe_b64decode(cursor + '=' * (-len(cursor) % 4)))
        if isinstance(value, list) and len(value) == 3:
            return value
    except (ValueError, TypeError):
        pass
    ctx.skip('the cursor is not a base64 JSON [owner, revision, offset], so owner, revision, offset and precedence checks that need '
             'a well-formed token cannot be crafted', name='task-artifacts.error_reasons')
    return None


def with_member(value, index, replacement):
    return encode(value[:index] + [replacement] + value[index + 1:])


@case('task-artifacts.error_reasons', CORE + ART)
def _(ctx):
    ctx.seed_reads()
    ctx.fx.write_artifacts(A, [artifact(ART1, 'a.txt'), artifact(ART2, 'b.txt')])
    ctx.rebase()
    first = success(ctx, ctx.ctl('task-artifacts', A, '--json', '--limit', '1'))
    cursor = first['next_cursor']
    # Shape: malformed, oversized, undecodable or wrongly typed.
    for label, token in (('malformed', 'zzz'), ('oversized', 'A' * 600), ('not JSON', encode('x')[:-1] + 'A'), ('an object', encode({'a': 1})),
                         ('a short list', encode([A, 'x'])), ('wrong types', encode([1, 'x', 0])), ('negative offset', encode([A, 'x', -1])),
                         ('a non-integer offset', encode([A, 'x', 'one'])), ('a boolean offset', encode([A, 'x', True]))):
        reason(ctx, ctx.ctl('task-artifacts', A, '--json', '--cursor', token), 'cursor_invalid', f'cursor {label}')
    parts = decode(ctx, cursor)
    if parts is not None:
        # Shape precedes binding: a token with a negative offset is invalid even though it is also stale.
        reason(ctx, ctx.ctl('task-artifacts', A, '--json', '--cursor', encode([parts[0], 'old', -5])), 'cursor_invalid', 'a stale cursor with a negative offset')
    # Binding: another owner, or another manifest revision.
    reason(ctx, ctx.ctl('task-artifacts', C, '--json', '--cursor', cursor), 'cursor_stale', 'cursor of another task')
    if parts is not None:
        other = with_member(parts, 0, C)
        reason(ctx, ctx.ctl('task-artifacts', A, '--json', '--cursor', other), 'cursor_stale', 'cursor naming another owner')
    # Offset range, against the current manifest.
    if parts is not None:
        far, edge = with_member(parts, 2, 99), with_member(parts, 2, 2)
        reason(ctx, ctx.ctl('task-artifacts', A, '--json', '--cursor', far), 'cursor_invalid', 'offset beyond the manifest')
        at_end = success(ctx, ctx.ctl('task-artifacts', A, '--json', '--cursor', edge))
        ctx.eq((at_end['items'], at_end['has_more'], at_end['next_cursor'], at_end['count']), ([], False, None, 2), 'an offset equal to the count is the empty end page [contract]')
    # Manifest change: the old cursor is stale, and staleness wins over its (now out of range) offset.
    ctx.fx.write_artifacts(A, [artifact(ART1, 'a.txt')])
    ctx.rebase()
    reason(ctx, ctx.ctl('task-artifacts', A, '--json', '--limit', '1', '--cursor', cursor), 'cursor_stale', 'cursor after the manifest changed')
    if parts is not None:
        reason(ctx, ctx.ctl('task-artifacts', A, '--json', '--cursor', far), 'cursor_stale', 'a stale cursor with an out-of-range offset')
    # Limit and other errors carry no reason, and the limit is checked before the cursor.
    for label, args in (('limit 0', ['--limit', '0']), ('limit 51', ['--limit', '51']), ('limit 0 with a bad cursor', ['--limit', '0', '--cursor', 'zzz'])):
        reason(ctx, ctx.ctl('task-artifacts', A, '--json', *args), '<absent>', label)
    reason(ctx, ctx.ctl('task-artifacts', MISSING, '--json'), '<absent>', 'unknown task')
    reason(ctx, ctx.ctl('task-artifacts', MISSING, '--json', '--cursor', 'zzz'), '<absent>', 'unknown task with a bad cursor')
