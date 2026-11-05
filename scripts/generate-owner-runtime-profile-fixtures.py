#!/usr/bin/env python3
"""Generate/check standalone synthetic owner-runtime candidate assets only."""
import argparse
from pathlib import Path
import sys

from owner_runtime_profile.tooling import check, generate


def main():
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='verify exact bytes without writing')
    parser.add_argument('--output-dir', type=Path, default=root / 'contracts/candidates/owner-runtime-v1')
    args = parser.parse_args()
    specifications = tuple(root / 'docs/decisions' / name for name in ('owner-runtime-v1.md', 'owner-runtime-v1-schema.md'))
    try:
        (check if args.check else generate)(args.output_dir, specifications)
    except (ValueError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 1
    print('owner-runtime candidate ' + ('verified' if args.check else 'generated'))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
