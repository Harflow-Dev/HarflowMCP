#!/usr/bin/env python3
"""Reject legacy product literals outside reviewed, file-specific exceptions."""
from collections import Counter
from pathlib import Path
import json
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
RULE = re.compile(r'polybridge|PB_[A-Z_]+|pb_task_id', re.IGNORECASE)
ALLOWLIST = ROOT / 'scripts/legacy-name-allowlist.json'

def main():
    entries = json.loads(ALLOWLIST.read_text())
    allowed = {(row['file'], row['literal']): row['count'] for row in entries if row.get('reason')}
    paths = subprocess.check_output(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'], cwd=ROOT).decode().split('\0')
    failures = []
    for name in sorted(set(paths)):
        if not name or name in ('scripts/legacy-name-allowlist.json', 'scripts/check-legacy-names.py'):
            continue
        path = ROOT / name
        if not path.is_file():
            continue
        try:
            content = path.read_text()
        except UnicodeError:
            continue
        observed = Counter(RULE.findall(content))
        for literal, count in observed.items():
            if (name, literal) not in allowed:
                failures.append(f'{name}: unapproved legacy literal {literal}')
            elif count != allowed[name, literal]:
                failures.append(f'{name}: legacy literal {literal} count {count}, expected {allowed[name, literal]}')
        for (file, literal), count in allowed.items():
            if file == name and literal not in observed and count:
                failures.append(f'{name}: missing expected legacy literal {literal}')
    if failures:
        print('\n'.join(failures), file=sys.stderr)
    return bool(failures)

if __name__ == '__main__':
    sys.exit(main())
