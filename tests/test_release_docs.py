"""Release-docs contract for the 1.3.0 release: docs, examples, and metadata stay in step.

The release ticket's acceptance criteria are documentation-shaped, so the suite treats them as
testable claims: the README installs the package, names every declared extra, records the baseline,
and every snippet marked runnable in the README or the usage guide actually runs.
"""

from __future__ import annotations

import importlib
import re
import tomllib
from pathlib import Path

import pytest

from pi_durable import __version__

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
USAGE = ROOT / "docs" / "usage.md"
PYPROJECT = ROOT / "pyproject.toml"

EXAMPLE_MARKER = "<!-- example -->"

#: The modules the README's API table must cover; a new public subpath belongs in the table.
PUBLIC_MODULES = (
    "pi_durable",
    "pi_durable.env",
    "pi_durable.harness",
    "pi_durable.providers",
    "pi_durable.session",
    "pi_durable.storage",
    "pi_durable.testing",
    "pi_durable.tool_search",
    "pi_durable.tools",
)


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def project() -> dict[str, object]:
    return tomllib.loads(read(PYPROJECT))["project"]


def declared_extras() -> dict[str, list[str]]:
    return tomllib.loads(read(PYPROJECT))["project"]["optional-dependencies"]


#: Prose spellings for distribution names the extras table describes rather than names.
EXTRA_LABELS = {"jsonschema": "schema", "pytest-asyncio": "pytest"}


def extra_rows() -> dict[str, str]:
    """The README extras table as extra name -> the lowercased description cell."""

    declared = declared_extras()
    rows: dict[str, str] = {}
    for line in read(README).splitlines():
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 3:
            continue
        match = re.fullmatch(r"`([a-z]+)`", cells[0])
        if match and match.group(1) in declared:
            rows[match.group(1)] = cells[1].lower()
    return rows


def requirements(name: str) -> list[str]:
    """Distribution names of one extra's requirements, without version specifiers."""

    return [re.split(r"[<>=!~\[ ]", spec, maxsplit=1)[0].lower() for spec in declared_extras()[name]]


def examples(path: Path) -> list[tuple[int, str]]:
    """Every fenced python block that follows an example marker, as (line, source)."""

    lines = read(path).splitlines()
    found: list[tuple[int, str]] = []
    index = 0
    while index < len(lines):
        if lines[index].strip() != EXAMPLE_MARKER:
            index += 1
            continue
        opening = index + 1
        assert opening < len(lines), f"{path}: example marker ends the file"
        assert lines[opening].strip() == "```python", (
            f"{path}:{opening + 1}: an example marker must be followed by a python block"
        )
        closing = opening + 1
        while closing < len(lines) and lines[closing].strip() != "```":
            closing += 1
        assert closing < len(lines), f"{path}:{opening + 1}: unclosed example block"
        found.append((opening + 2, "\n".join(lines[opening + 1 : closing])))
        index = closing + 1
    return found


def test_packaging_version_agrees_with_the_package() -> None:
    # The lockstep literal lives in tests/test_scaffold.py; this checks the artifact metadata agrees.
    assert project()["version"] == __version__


def test_readme_documents_installation() -> None:
    text = read(README)
    assert re.search(r"^## Install$", text, re.MULTILINE), "README has no Install section"
    assert "pip install pi-durable-python" in text
    assert "requires-python" not in text  # the reader wants a version range, not the key
    assert "3.11" in text


def test_readme_names_every_extra() -> None:
    text = read(README)
    documented = set(re.findall(r"pi-durable-python\[([a-z]+)\]", text))
    declared = set(declared_extras())
    assert documented == declared, (
        "README extras and pyproject optional-dependencies disagree: "
        f"only in README {documented - declared}, "
        f"only in pyproject {declared - documented}"
    )


def test_readme_describes_each_extra() -> None:
    rows = extra_rows()
    for name in declared_extras():
        assert name in rows, f"README extras table has no row for {name}"
        for requirement in requirements(name):
            label = EXTRA_LABELS.get(requirement, requirement)
            assert label in rows[name], f"README row for {name} does not describe {requirement}"


def test_readme_records_the_baseline() -> None:
    text = read(README)
    version = str(project()["version"])
    assert f"v{version}" in text
    assert "71023ae" in text, "README does not name the upstream commit the port tracks"
    assert "earendil-works/pi-durable" in text


def test_readme_links_the_usage_docs() -> None:
    text = read(README)
    assert USAGE.is_file(), "docs/usage.md is missing"
    assert "docs/usage.md" in text
    assert "PARITY.md" in text


def test_readme_covers_the_public_modules() -> None:
    text = read(README)
    listed = set(re.findall(r"`(pi_durable(?:\.[a-z_]+)*)`", text))
    missing = set(PUBLIC_MODULES) - listed
    assert not missing, f"README API table does not cover {sorted(missing)}"
    for module in PUBLIC_MODULES:
        importlib.import_module(module)


def test_readme_states_the_runtime_contract() -> None:
    text = read(README)
    assert "free-threaded" in text, "README does not state the free-threaded (GIL-disabled) contract"
    assert re.search(r"no runtime dependencies", text), "README does not state the core dependency rule"


@pytest.mark.parametrize("path", [README, USAGE], ids=["README", "usage"])
def test_documented_examples_run(path: Path) -> None:
    blocks = examples(path)
    assert blocks, f"{path} has no runnable examples"
    for line, source in blocks:
        where = f"{path.relative_to(ROOT)}:{line}"
        exec(compile(source, where, "exec"), {"__name__": "__example__"})
