"""Baseline contract: what the tree declares before any of it is built.

Declaring a baseline is naming the contract the port is about to satisfy: the vendored upstream
files and their provenance, the reference that records them, the parity matrix that names the same
baseline, and the decision records that bound the scope. These tests read those artifacts the way
the next ticket reads them, so a bump that vendors a spec but leaves the reference, the matrix or
the exclusions behind fails here rather than in a later audit.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM = ROOT / "docs" / "upstream"
REFERENCE = UPSTREAM / "README.md"
PARITY = ROOT / "PARITY.md"
ADR_DIR = ROOT / "docs" / "adr"
TESTS = ROOT / "tests"

#: How a matrix row cites a translated suite: by file name, in its Tests cell or, for a port
#: addition, in its Status cell.
CITED_SUITE = re.compile(r"\btest_[A-Za-z0-9_]+\.py\b")

#: The vendored baselines of earlier releases. They are the audit record of those ports, so their
#: bytes are frozen here at the values the v1.5.0 baseline declaration found them at.
AUDIT_RECORDS = {
    "spec-1.3.0.md": "5d36a665c22171cda28ed4170b8464b49a9464e41f8da7baa85021c5b3f6de6a",
    "spec-1.4.0.md": "d5938165ea79877f1a1bef74c3ca21a2c34b28c614bc122c6a5c9ffbb9828dff",
}

#: The fields the upstream reference must carry, per docs/upstream/README.md's own contract.
REFERENCE_FIELDS = ("Package", "Version", "Tag", "Commit", "Source", "Retrieved", "License")


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def reference_field(name: str) -> str:
    """One field of the upstream reference block, as ``Version: `1.5.0`` yields ``1.5.0``."""

    match = re.search(rf"^-\s+{name}:\s*(.+?)\s*$", read(REFERENCE), re.MULTILINE)
    assert match is not None, f"docs/upstream/README.md records no {name}"
    return match.group(1).strip().strip("`").strip("<>")


def header_field(text: str, name: str) -> str:
    """One field of a vendored file's provenance header."""

    match = re.search(rf"^{name}:\s*(.+?)\s*$", text, re.MULTILINE)
    assert match is not None, f"the provenance header records no {name}"
    return match.group(1).strip()


def vendored(version: str, stem: str) -> Path:
    return UPSTREAM / f"{stem}-{version}.md"


def provenance_header(path: Path) -> str:
    match = re.match(r"<!--\n(.*?)\n-->", read(path), re.DOTALL)
    assert match is not None, f"{path.name} has no provenance header"
    return match.group(1)


def test_upstream_reference_records_the_provenance() -> None:
    for name in REFERENCE_FIELDS:
        assert reference_field(name), f"docs/upstream/README.md leaves {name} empty"
    assert reference_field("Package") == "@earendil-works/pi-durable"
    version = reference_field("Version")
    assert re.fullmatch(r"\d+\.\d+\.\d+", version), f"version {version} is not X.Y.Z"
    assert reference_field("Tag") == f"v{version}", "the tag does not carry the version"
    assert re.fullmatch(r"[0-9a-f]{40}", reference_field("Commit")), "the commit is not a full sha"
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", reference_field("Retrieved")), (
        "the retrieval date is not YYYY-MM-DD"
    )
    assert reference_field("Source") == (
        f"https://github.com/earendil-works/pi/tree/v{version}/packages/durable"
    )


def test_the_vendored_specification_carries_the_reference_provenance() -> None:
    version = reference_field("Version")
    path = vendored(version, "spec")
    assert path.is_file(), f"the v{version} specification is not vendored at {path.relative_to(ROOT)}"
    header = provenance_header(path)
    for name, expected in (
        ("Tag", reference_field("Tag")),
        ("Commit", reference_field("Commit")),
        ("Retrieved", reference_field("Retrieved")),
    ):
        assert header_field(header, name) == expected, (
            f"{path.name} records {name} {header_field(header, name)!r}, the reference records "
            f"{expected!r}"
        )
    assert header_field(header, "Source") == (
        f"https://github.com/earendil-works/pi/blob/v{version}/packages/durable/docs/spec.md"
    )
    assert "Do not edit" in header, f"{path.name} does not say the vendored file is not to be edited"


def test_the_vendored_group_commit_document_carries_the_reference_provenance() -> None:
    version = reference_field("Version")
    path = vendored(version, "group-commit")
    assert path.is_file(), (
        f"the v{version} group-commit document is not vendored at {path.relative_to(ROOT)}"
    )
    header = provenance_header(path)
    assert header_field(header, "Tag") == reference_field("Tag")
    assert header_field(header, "Commit") == reference_field("Commit")
    assert header_field(header, "Retrieved") == reference_field("Retrieved")
    assert header_field(header, "Source") == (
        f"https://github.com/earendil-works/pi/blob/v{version}/packages/durable/docs/group-commit.md"
    )
    assert "Do not edit" in header, f"{path.name} does not say the vendored file is not to be edited"


def test_the_reference_lists_every_vendored_file() -> None:
    files = read(REFERENCE).split("## Files", 1)[-1].split("## ", 1)[0]
    listed = set(re.findall(r"`([A-Za-z0-9._-]+\.md)`", files))
    on_disk = {path.name for path in UPSTREAM.glob("*.md")} - {"README.md"}
    assert listed == on_disk, (
        "docs/upstream/README.md's Files section and the vendored directory disagree: "
        f"only listed {sorted(listed - on_disk)}, only on disk {sorted(on_disk - listed)}"
    )


def test_the_earlier_vendored_baselines_are_untouched() -> None:
    for name, digest in AUDIT_RECORDS.items():
        path = UPSTREAM / name
        assert path.is_file(), f"{name} is the audit record of an earlier port and must be kept"
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        assert actual == digest, (
            f"{name} changed; an earlier vendored baseline is the audit record of that release "
            "and a sync does not rewrite it"
        )


# --- The matrix names the same baseline -------------------------------------------------------

#: The names each upstream entry file exports at tag v1.5.0 that it did not export at v1.4.0,
#: transcribed from the cached v1.5.0 `src/**` and the v1.4.0 entry files fetched from the
#: reference's source link. The matrix owes each of these a row, whatever its status.
ADDED_UPSTREAM_EXPORTS: dict[str, tuple[str, ...]] = {
    "./errors.ts": ("ConversationIdle", "ReadOnlySession"),
    "./harness/define.ts": ("runEndTask",),
    "./harness/live.ts": ("LiveRun", "RunFailure", "RunFailureReason"),
    "./harness/registry-reader.ts": ("registryReader",),
    "./harness/transcript.ts": (
        "ConversationTranscript",
        "ConversationTranscriptOptions",
        "EntryLoader",
        "TranscriptItem",
        "TranscriptView",
    ),
    "./harness/types.ts": ("RunEndTask", "RunSummary"),
    "./harness/view.ts": ("ConversationWindow",),
    "./types.ts": (
        "CommitObservation",
        "CommitObserver",
        "CommitOptions",
        "CommitPersistence",
        "CommitResult",
        "NativeWatchCall",
        "Observation",
        "Observer",
        "Report",
        "WatchCall",
    ),
    "src/env/index.ts": ("FileChange",),
    "src/testing/index.ts": ("commitWrites",),
}

#: The storage subpath v1.5.0 adds. ADR 0003 excludes it, so the matrix owes it an
#: Excluded-subpaths row rather than an export row.
ADDED_UPSTREAM_SUBPATH = "./storage/store"

#: Statuses a matrix row may carry, per the legend at the top of PARITY.md.
MATRIX_STATUSES = ("planned", "code", "tested", "excluded", "n/a")


def matrix_rows(heading_key: str) -> set[str]:
    """The first cell of every table row under the matrix subsection headed by ``heading_key``."""

    rows: set[str] = set()
    inside = False
    for line in read(PARITY).splitlines():
        if line.startswith("#"):
            inside = line.startswith(f"### `{heading_key}`")
        elif inside and line.startswith("| `"):
            rows.add(line.split("`")[1])
    return rows


def matrix_table(heading: str) -> list[list[str]]:
    """The cells of every table row under a level-2 matrix section."""

    body = read(PARITY).split(f"## {heading}", 1)[-1].split("\n## ", 1)[0]
    rows = []
    for line in body.splitlines():
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) >= 2 and cells[0] and not set(cells[0]) <= {"-"}:
            rows.append(cells)
    return rows


def cited_suites() -> list[tuple[str, str]]:
    """Every ``(row, suite)`` pair the matrix cites, keyed by the row that cites it.

    A row cites a suite by file name, so the citation is a claim about the tree: it is resolved
    here rather than read by eye, which is what lets a row name a suite that was never written.
    """

    citations: list[tuple[str, str]] = []
    for heading in ("Root exports", "Optional subpaths"):
        body = read(PARITY).split(f"## {heading}", 1)[-1].split("\n## Excluded", 1)[0]
        for line in body.splitlines():
            if not line.startswith("| `"):
                continue
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if len(cells) < 5:
                continue
            row = f"{heading} / {cells[0]}"
            citations += [(row, suite) for suite in CITED_SUITE.findall(cells[3])]
    for cells in matrix_table("Port additions"):
        row = f"Port additions / {cells[0]}"
        citations += [(row, suite) for suite in CITED_SUITE.findall(" ".join(cells))]
    return citations


def spec_section_status() -> dict[str, str]:
    """The `## Spec sections` table as section number -> status cell."""

    statuses: dict[str, str] = {}
    for cells in matrix_table("Spec sections"):
        number = cells[0].split(" ", 1)[0]
        if re.fullmatch(r"\d+(?:\.\d+)*", number):
            statuses[number] = cells[-1]
    return statuses


def vendored_spec_sections(version: str) -> set[str]:
    """Every section number the vendored specification carries."""

    text = read(vendored(version, "spec"))
    pattern = r"^#{2,4}\s+(\d+(?:\.\d+)*)[.\s]"
    return {match.group(1) for match in re.finditer(pattern, text, re.MULTILINE)}


def test_the_matrix_names_the_vendored_baseline() -> None:
    version, tag, commit = (reference_field(name) for name in ("Version", "Tag", "Commit"))
    text = read(PARITY)
    baseline = re.search(r"\*\*Baseline:\*\*\s+(.+?)(?:\n\n|\n(?=\*\*))", text, re.DOTALL)
    assert baseline is not None, "PARITY.md states no Baseline line"
    stated = " ".join(baseline.group(1).split())
    for expected in (version, tag, commit):
        assert expected in stated, f"PARITY.md names baseline {stated!r}, not {expected}"
    assert f"docs/upstream/spec-{version}.md" in text, (
        f"PARITY.md's normative reference is not the vendored v{version} specification"
    )
    assert f"docs/upstream/group-commit-{version}.md" in text, (
        f"PARITY.md does not name the vendored v{version} group-commit document"
    )


def test_the_matrix_rows_every_vendored_spec_section() -> None:
    version = reference_field("Version")
    missing = sorted(vendored_spec_sections(version) - set(spec_section_status()))
    assert not missing, f"PARITY.md's Spec sections table has no row for {', '.join(missing)}"


def test_the_matrix_rows_every_new_upstream_export() -> None:
    missing = {
        module: sorted(set(names) - matrix_rows(module))
        for module, names in ADDED_UPSTREAM_EXPORTS.items()
        if set(names) - matrix_rows(module)
    }
    assert not missing, (
        "these v1.5.0 exports have no row in PARITY.md: "
        + "; ".join(f"{module}: {', '.join(names)}" for module, names in sorted(missing.items()))
    )


def test_the_matrix_excludes_the_subpaths_this_baseline_added() -> None:
    rows = matrix_table("Excluded subpaths")
    reasons = {cells[0].strip("`"): " ".join(cells[1:]) for cells in rows}
    assert ADDED_UPSTREAM_SUBPATH in reasons, (
        f"{ADDED_UPSTREAM_SUBPATH} is a v1.5.0 subpath PARITY.md neither ports nor excludes"
    )
    assert "0003" in reasons[ADDED_UPSTREAM_SUBPATH], (
        f"the reason for {ADDED_UPSTREAM_SUBPATH} does not point at the decision that excludes it"
    )


def test_a_matrix_row_without_a_suite_is_not_marked_as_one() -> None:
    """A row's status and its Tests cell agree: `planned` and `excluded` cite no suite."""

    wrong: list[str] = []
    for heading in ("Root exports", "Optional subpaths"):
        body = read(PARITY).split(f"## {heading}", 1)[-1].split("\n## Excluded", 1)[0]
        for line in body.splitlines():
            if not line.startswith("| `"):
                continue
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if len(cells) < 5:
                continue
            status = cells[-1].split(" ", 1)[0]
            cites_a_suite = cells[3].startswith("test_")
            assert status in MATRIX_STATUSES, f"{cells[0]} carries the unknown status {status!r}"
            if status in ("planned", "excluded") and cites_a_suite:
                wrong.append(f"{cells[0]} ({status}, cites {cells[3]})")
    assert not wrong, f"these rows cite a translated suite without being tested: {wrong}"


def test_every_cited_test_file_exists() -> None:
    """A `tested` row may only cite a suite the tree actually holds.

    The status and the Tests cell agreeing is half the contract; the other half is that the named
    file is there to run. A row that cites a suite nobody wrote is an overclaim no suite can
    contradict, so the citation is resolved against `tests/` here.
    """

    missing = sorted({(row, suite) for row, suite in cited_suites() if not (TESTS / suite).is_file()})
    assert not missing, (
        "these matrix rows cite a suite that is not in tests/: "
        + "; ".join(f"{row} cites {suite}" for row, suite in missing)
    )


def test_every_port_additions_row_has_the_table_arity() -> None:
    """A port addition is on the register only if it renders as one: a row of the header's cells.

    A row written with more cells than the `Addition | Module | Status` header has does not render
    as a table row at all, so no reader and no check of the rendered matrix ever sees it.
    """

    wrong = [cells for cells in matrix_table("Port additions") if len(cells) != 3]
    assert not wrong, (
        "these Port additions rows do not render as the 3-cell table rows the section's header "
        f"declares: {'; '.join(' | '.join(cells) for cells in wrong)}"
    )


def test_every_spec_section_row_has_the_table_arity() -> None:
    """A spec-section claim counts only if it renders as one: a row of the header's two cells.

    A second row appended to a row with `||` is one physical line holding two rows, so the rendered
    table shows it as one cell run and `spec_section_status()` reads the statuses concatenated.
    The `tested` claim of the row before it then rests on the text of the row smuggled into it, which
    no reader of the rendered matrix and no check of its cells can see.
    """

    wrong = [cells for cells in matrix_table("Spec sections") if len(cells) != 2]
    joined = "; ".join(" | ".join(cell[:40] for cell in cells) for cells in wrong)
    assert not wrong, (
        "these Spec sections rows do not render as the 2-cell table rows the section's header "
        f"declares: {joined}"
    )


# --- The decision records bound this baseline -------------------------------------------------

ADR_0001 = ADR_DIR / "0001-parity-baseline-and-boundary.md"
ADR_0003 = ADR_DIR / "0003-upstream-1-5-0-scope.md"

#: The four exclusions this baseline records, each with the words its reason must carry so a reader
#: learns why the port does not have it, not merely that it does not.
ADR_0003_EXCLUSIONS = {
    "attached-host storage subpath": ("./storage/store", "Chord"),
    "pi-schema": ("JSON Schema", "parameters"),
    "telemetry host-callback member": ("telemetry", "HostCallback"),
    "everything ADR 0002 already excluded": ("0002", "Code Mode"),
}


def test_adr_0003_records_this_baseline() -> None:
    version, tag, commit = (reference_field(name) for name in ("Version", "Tag", "Commit"))
    assert ADR_0003.is_file(), "this baseline's scope decisions have no decision record"
    text = read(ADR_0003)
    for expected in (version, tag, commit):
        assert expected in text, f"{ADR_0003.name} does not name {expected}"
    assert f"spec-{version}.md" in text, f"{ADR_0003.name} does not link the vendored specification"


def test_adr_0003_records_the_four_exclusions_with_their_reasons() -> None:
    text = read(ADR_0003).lower()
    for exclusion, words in ADR_0003_EXCLUSIONS.items():
        assert exclusion.lower() in text, f"{ADR_0003.name} does not exclude the {exclusion}"
        missing = [word for word in words if word.lower() not in text]
        assert not missing, (
            f"{ADR_0003.name} excludes the {exclusion} without naming {', '.join(missing)}"
        )


def test_the_first_decision_record_is_superseded_in_part_not_rewritten() -> None:
    text = read(ADR_0001)
    assert re.search(r"^\*\*Superseded in part:\*\*", text, re.MULTILINE), (
        "ADR 0001 carries no superseded-in-part line"
    )
    assert "0003" in text, "ADR 0001's superseded-in-part line does not name the record that bounds it"
    assert "**Status:** accepted" in text, "ADR 0001 no longer states its own status"
    assert "v1.3.0" in text and "71023aec1546d6d677a65669c5665c0927ce0dd0" in text, (
        "ADR 0001's decision text was rewritten; a later baseline supersedes it in part instead"
    )
