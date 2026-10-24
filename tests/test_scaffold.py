"""Scaffold contract: the package version follows the vendored upstream baseline.

The port releases in lockstep with `@earendil-works/pi-durable`, so `__version__` must equal the
upstream version vendored under `docs/upstream/`. A sync bumps both together; neither hardcodes the
current release here.
"""

from __future__ import annotations

import re
from pathlib import Path

from pi_durable import __version__

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM = ROOT / "docs" / "upstream" / "README.md"


def upstream_version() -> str:
    text = UPSTREAM.read_text(encoding="utf-8")
    match = re.search(r"^-\s+Version:\s*`([^`]+)`\s*$", text, re.MULTILINE)
    assert match is not None, "docs/upstream/README.md must record the vendored upstream version"
    return match.group(1)


def test_version_tracks_the_upstream_baseline() -> None:
    assert __version__ == upstream_version(), (
        "src/pi_durable/__init__.py and docs/upstream/README.md disagree; "
        "bump both when syncing an upstream release"
    )
