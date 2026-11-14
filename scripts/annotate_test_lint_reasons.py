#!/usr/bin/env python3
"""Attach a reason to every crate-level test lint relaxation.

This is a uniform, enumerated, text-level transform over a known set of files.
It is deliberately *not* a Rust rewriter: it matches two exact shapes, refuses
to touch anything else, and the result is verified afterwards by
`cargo clippy --all-targets -- -D warnings`, which proves every attribute still
parses and still applies.
"""

import sys
from pathlib import Path

BLOCK = """#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]"""

BLOCK_NEW = """#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    reason = "integration tests drive failure paths with unwrap/expect/panic and print diagnostics"
)]
#![allow(
    clippy::allow_attributes,
    reason = "test crates relax the production lint set wholesale; `expect` cannot express lints that may not fire"
)]"""

SINGLE = "#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]"

SINGLE_NEW = """#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test helpers accept unwraps the same way `#[cfg(test)]` modules do"
)]
#![allow(
    clippy::allow_attributes,
    reason = "test crates relax the production lint set wholesale; `expect` cannot express lints that may not fire"
)]"""


def main(paths: list[str]) -> int:
    for path in map(Path, paths):
        text = path.read_text()
        if text.count(BLOCK) == 1:
            path.write_text(text.replace(BLOCK, BLOCK_NEW))
        elif text.count(SINGLE) == 1:
            path.write_text(text.replace(SINGLE, SINGLE_NEW))
        else:
            print(f"skip (unexpected shape): {path}", file=sys.stderr)
            return 1
        print(f"updated {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
