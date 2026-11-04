"""Standalone deterministic fake. Its observation is untrusted corroboration only."""
import argparse
import json
import os
import sys
import time


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('version', 'success', 'nonzero', 'hang', 'flood', 'pipe-holder'))
    parser.add_argument('--nonce')
    parser.add_argument('--observation')
    args = parser.parse_args(argv)
    if args.observation:
        with open(args.observation, 'x') as stream:
            json.dump({'pid': os.getpid(), 'nonce': args.nonce or os.environ.get('CONFORMANCE_NONCE'), 'argv': sys.argv[1:]}, stream)
    if args.mode == 'version':
        print('harflow-conformance-fake 1')
    elif args.mode in ('success', 'nonzero'):
        print(json.dumps({'type': 'text', 'text': 'deterministic fake output'}), flush=True)
        print(json.dumps({'type': 'result', 'result': 'ok' if args.mode == 'success' else 'failed'}), flush=True)
        return 0 if args.mode == 'success' else 7
    elif args.mode == 'hang':
        while True:
            time.sleep(1)
    elif args.mode == 'flood':
        while True:
            os.write(1, b'x' * 4096)
            os.write(2, b'y' * 4096)
    elif args.mode == 'pipe-holder':
        if os.fork() == 0:
            while True:
                time.sleep(1)
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
