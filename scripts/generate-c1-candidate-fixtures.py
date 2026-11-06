#!/usr/bin/env python3
"""Generate or check the standalone C1 candidate assets (result, survivors, permission mode)."""
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from c1_candidates import common, permission, result, survivors  # noqa: E402

CANDIDATES = {c['name']: c for c in (result.CANDIDATE, survivors.CANDIDATE, permission.CANDIDATE)}


def main():
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='verify exact bytes without writing')
    parser.add_argument('--only', choices=sorted(CANDIDATES), action='append')
    args = parser.parse_args()
    try:
        for name in args.only or sorted(CANDIDATES):
            (common.check if args.check else common.generate)(CANDIDATES[name], root)
    except (ValueError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 1
    print('C1 candidates ' + ('verified' if args.check else 'generated'))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
