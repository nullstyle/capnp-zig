#!/usr/bin/env python3
"""Compatibility entry point for mise's compiler bootstrap and verification."""

import argparse
import sys

from capnp_tool import cli


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    sys.exit(cli(["verify" if args.verify else "bootstrap"]))
