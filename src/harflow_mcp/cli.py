"""Lazy console dispatch; version-info never imports the task runtime."""
from __future__ import annotations

import argparse
import json
import sys

from .wire_contracts import PRODUCT_VERSION, VERSION_INFO_FORMAT, envelope, version_info


class _VersionParser(argparse.ArgumentParser):
    def __init__(self, *, json_requested: bool):
        super().__init__(prog="harflow-ctl version-info")
        self.json_requested = json_requested

    def error(self, message: str) -> None:
        if self.json_requested:
            print(json.dumps(envelope(VERSION_INFO_FORMAT, {"error": {"code": "usage", "message": message}})))
        print(f"{self.prog}: error: {message}", file=sys.stderr)
        raise SystemExit(2)


def version_info_main(argv: list[str]) -> int:
    parser = _VersionParser(json_requested="--json" in argv)
    parser.add_argument("--json", action="store_true", help="print the Harflow v1 compatibility document, including the selected HARFLOW_STATE_ROOT or home default")
    args = parser.parse_args(argv)
    from .state_layout import InvalidStateRoot
    try:
        info = version_info()
    except InvalidStateRoot as exc:
        parser.error(str(exc))
    if args.json:
        print(json.dumps(info))
    else:
        print(f"HarflowMCP {PRODUCT_VERSION}")
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == "version-info":
        return version_info_main(arguments[1:])
    from . import ctl
    return ctl.main(arguments)


if __name__ == "__main__":
    raise SystemExit(main())
