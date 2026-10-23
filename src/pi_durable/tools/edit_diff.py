"""Exact-text edit matching, fuzzy normalization, and diff rendering.

Port of the v1.3.0 ``src/tools/edit-diff.ts``. The JavaScript implementation uses the ``diff``
package; the Python port uses :mod:`difflib` and renders the same patch and display formats.
"""

from __future__ import annotations

import difflib
import re
import unicodedata
from dataclasses import dataclass
from typing import Any


def detect_line_ending(content: str) -> str:
    """The file's line ending: ``\\r\\n`` when it comes first, otherwise ``\\n``."""

    crlf_index = content.find("\r\n")
    lf_index = content.find("\n")
    if lf_index == -1 or crlf_index == -1:
        return "\n"
    return "\r\n" if crlf_index < lf_index else "\n"


def normalize_to_lf(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def restore_line_endings(text: str, ending: str) -> str:
    return text.replace("\n", "\r\n") if ending == "\r\n" else text


def normalize_for_fuzzy_match(text: str) -> str:
    """Strip trailing whitespace and normalize quotes, dashes, and special spaces."""

    normalized = unicodedata.normalize("NFKC", text)
    normalized = "\n".join(line.rstrip() for line in normalized.split("\n"))
    normalized = re.sub("[\u2018\u2019\u201a\u201b]", "'", normalized)
    normalized = re.sub("[\u201c\u201d\u201e\u201f]", '"', normalized)
    normalized = re.sub("[\u2010\u2011\u2012\u2013\u2014\u2015\u2212]", "-", normalized)
    normalized = re.sub("[\u00a0\u2002-\u200a\u202f\u205f\u3000]", " ", normalized)
    return normalized


def _split_lines_with_endings(content: str) -> list[str]:
    return re.findall(r"[^\n]*\n|[^\n]+", content)


@dataclass(slots=True)
class _LineSpan:
    start: int
    end: int


@dataclass(slots=True)
class _Replacement:
    match_index: int
    match_length: int
    new_text: str


@dataclass(slots=True)
class MatchedEdit:
    edit_index: int
    match_index: int
    match_length: int
    new_text: str


def _line_spans(content: str) -> list[_LineSpan]:
    spans: list[_LineSpan] = []
    offset = 0
    for line in _split_lines_with_endings(content):
        spans.append(_LineSpan(offset, offset + len(line)))
        offset += len(line)
    return spans


def _replacement_line_range(lines: list[_LineSpan], replacement: _Replacement) -> tuple[int, int]:
    replacement_start = replacement.match_index
    replacement_end = replacement.match_index + replacement.match_length

    start_line = -1
    for index, line in enumerate(lines):
        if line.start <= replacement_start < line.end:
            start_line = index
            break
    if start_line == -1:
        raise ValueError("Replacement range is outside the base content.")

    end_line = start_line
    while end_line < len(lines) and lines[end_line].end < replacement_end:
        end_line += 1
    if end_line >= len(lines):
        raise ValueError("Replacement range is outside the base content.")
    return start_line, end_line + 1


def _apply_replacements(content: str, replacements: list[_Replacement], offset: int = 0) -> str:
    result = content
    for replacement in reversed(replacements):
        match_index = replacement.match_index - offset
        result = (
            result[:match_index] + replacement.new_text + result[match_index + replacement.match_length :]
        )
    return result


def apply_replacements_preserving_unchanged_lines(
    original_content: str,
    base_content: str,
    replacements: list[_Replacement],
) -> str:
    """Apply normalized-view replacements while keeping untouched lines from the original."""

    original_lines = _split_lines_with_endings(original_content)
    base_lines = _line_spans(base_content)
    if len(original_lines) != len(base_lines):
        raise ValueError(
            "Cannot preserve unchanged lines because the base content has a different line count."
        )

    groups: list[dict[str, Any]] = []
    for replacement in sorted(replacements, key=lambda item: item.match_index):
        start_line, end_line = _replacement_line_range(base_lines, replacement)
        current = groups[-1] if groups else None
        if current is not None and start_line < current["end_line"]:
            current["end_line"] = max(current["end_line"], end_line)
            current["replacements"].append(replacement)
            continue
        groups.append({"start_line": start_line, "end_line": end_line, "replacements": [replacement]})

    original_line_index = 0
    result = ""
    for group in groups:
        result += "".join(original_lines[original_line_index : group["start_line"]])
        group_start = base_lines[group["start_line"]].start
        group_end = base_lines[group["end_line"] - 1].end
        result += _apply_replacements(base_content[group_start:group_end], group["replacements"], group_start)
        original_line_index = group["end_line"]
    result += "".join(original_lines[original_line_index:])
    return result


@dataclass(frozen=True, slots=True)
class FuzzyMatchResult:
    found: bool
    index: int
    match_length: int
    used_fuzzy_match: bool
    content_for_replacement: str


def fuzzy_find_text(content: str, old_text: str) -> FuzzyMatchResult:
    """Find ``old_text`` exactly first, then in the fuzzy-normalized content."""

    exact_index = content.find(old_text)
    if exact_index != -1:
        return FuzzyMatchResult(True, exact_index, len(old_text), False, content)

    fuzzy_content = normalize_for_fuzzy_match(content)
    fuzzy_old_text = normalize_for_fuzzy_match(old_text)
    fuzzy_index = fuzzy_content.find(fuzzy_old_text)
    if fuzzy_index == -1:
        return FuzzyMatchResult(False, -1, 0, False, content)
    return FuzzyMatchResult(True, fuzzy_index, len(fuzzy_old_text), True, fuzzy_content)


def strip_bom(content: str) -> tuple[str, str]:
    """Split a leading byte-order mark from the text."""

    if content.startswith("\ufeff"):
        return "\ufeff", content[1:]
    return "", content


def _count_occurrences(content: str, old_text: str) -> int:
    return len(normalize_for_fuzzy_match(content).split(normalize_for_fuzzy_match(old_text))) - 1


def _not_found_error(path: str, edit_index: int, total_edits: int) -> ValueError:
    if total_edits == 1:
        return ValueError(
            f"Could not find the exact text in {path}. The old text must match exactly including "
            "all whitespace and newlines."
        )
    return ValueError(
        f"Could not find edits[{edit_index}] in {path}. The oldText must match exactly including "
        "all whitespace and newlines."
    )


def _duplicate_error(path: str, edit_index: int, total_edits: int, occurrences: int) -> ValueError:
    if total_edits == 1:
        return ValueError(
            f"Found {occurrences} occurrences of the text in {path}. The text must be unique. "
            "Please provide more context to make it unique."
        )
    return ValueError(
        f"Found {occurrences} occurrences of edits[{edit_index}] in {path}. Each oldText must be "
        "unique. Please provide more context to make it unique."
    )


def _empty_old_text_error(path: str, edit_index: int, total_edits: int) -> ValueError:
    if total_edits == 1:
        return ValueError(f"oldText must not be empty in {path}.")
    return ValueError(f"edits[{edit_index}].oldText must not be empty in {path}.")


def _no_change_error(path: str, total_edits: int) -> ValueError:
    if total_edits == 1:
        return ValueError(
            f"No changes made to {path}. The replacement produced identical content. This might "
            "indicate an issue with special characters or the text not existing as expected."
        )
    return ValueError(f"No changes made to {path}. The replacements produced identical content.")


@dataclass(frozen=True, slots=True)
class AppliedEdits:
    base_content: str
    new_content: str


def apply_edits_to_normalized_content(
    normalized_content: str,
    edits: list[dict[str, str]],
    path: str,
) -> AppliedEdits:
    """Apply all edits to LF-normalized content; every edit matches the original content."""

    normalized_edits = [
        {"old_text": normalize_to_lf(edit["oldText"]), "new_text": normalize_to_lf(edit["newText"])}
        for edit in edits
    ]
    for index, edit in enumerate(normalized_edits):
        if edit["old_text"] == "":
            raise _empty_old_text_error(path, index, len(normalized_edits))

    initial_matches = [fuzzy_find_text(normalized_content, edit["old_text"]) for edit in normalized_edits]
    used_fuzzy_match = any(match.used_fuzzy_match for match in initial_matches)
    replacement_base = (
        normalize_for_fuzzy_match(normalized_content) if used_fuzzy_match else normalized_content
    )

    matched: list[MatchedEdit] = []
    for index, edit in enumerate(normalized_edits):
        match = fuzzy_find_text(replacement_base, edit["old_text"])
        if not match.found:
            raise _not_found_error(path, index, len(normalized_edits))
        occurrences = _count_occurrences(replacement_base, edit["old_text"])
        if occurrences > 1:
            raise _duplicate_error(path, index, len(normalized_edits), occurrences)
        matched.append(MatchedEdit(index, match.index, match.match_length, edit["new_text"]))

    matched.sort(key=lambda item: item.match_index)
    for previous, current in zip(matched, matched[1:], strict=False):
        if previous.match_index + previous.match_length > current.match_index:
            raise ValueError(
                f"edits[{previous.edit_index}] and edits[{current.edit_index}] overlap in "
                f"{path}. Merge them into one edit or target disjoint regions."
            )

    base_content = normalized_content
    replacements = [_Replacement(item.match_index, item.match_length, item.new_text) for item in matched]
    new_content = (
        apply_replacements_preserving_unchanged_lines(normalized_content, replacement_base, replacements)
        if used_fuzzy_match
        else _apply_replacements(replacement_base, replacements)
    )
    if base_content == new_content:
        raise _no_change_error(path, len(normalized_edits))
    return AppliedEdits(base_content, new_content)


def generate_unified_patch(path: str, old_content: str, new_content: str, context_lines: int = 4) -> str:
    """Generate a standard unified patch with the ``diff`` package's file-header-only format."""

    old_lines = _split_lines_with_endings(old_content)
    new_lines = _split_lines_with_endings(new_content)
    matcher = difflib.SequenceMatcher(None, old_lines, new_lines, autojunk=False)
    output = [f"--- {path}", f"+++ {path}"]

    def emit(prefix: str, line: str) -> None:
        if line.endswith("\n"):
            output.append(prefix + line[:-1])
        else:
            output.append(prefix + line)
            output.append("\\ No newline at end of file")

    for group in matcher.get_grouped_opcodes(context_lines):
        old_start, old_end = group[0][1], group[-1][2]
        new_start, new_end = group[0][3], group[-1][4]
        old_count = old_end - old_start
        new_count = new_end - new_start
        old_first = old_start + 1
        new_first = new_start + 1
        if old_count == 0:
            old_first -= 1
        if new_count == 0:
            new_first -= 1
        output.append(f"@@ -{old_first},{old_count} +{new_first},{new_count} @@")
        for tag, i1, i2, j1, j2 in group:
            if tag == "equal":
                for line in old_lines[i1:i2]:
                    emit(" ", line)
                continue
            for line in old_lines[i1:i2]:
                emit("-", line)
            for line in new_lines[j1:j2]:
                emit("+", line)
    return "\n".join(output) + "\n"


@dataclass(frozen=True, slots=True)
class DisplayDiff:
    diff: str
    first_changed_line: int | None


@dataclass(slots=True)
class _Part:
    value: str
    added: bool = False
    removed: bool = False


def _parts(old_content: str, new_content: str) -> list[_Part]:
    old_lines = old_content.splitlines(keepends=True)
    new_lines = new_content.splitlines(keepends=True)
    matcher = difflib.SequenceMatcher(None, old_lines, new_lines, autojunk=False)
    parts: list[_Part] = []
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            parts.append(_Part("".join(old_lines[i1:i2])))
        elif tag == "delete":
            parts.append(_Part("".join(old_lines[i1:i2]), removed=True))
        elif tag == "insert":
            parts.append(_Part("".join(new_lines[j1:j2]), added=True))
        else:
            parts.append(_Part("".join(old_lines[i1:i2]), removed=True))
            parts.append(_Part("".join(new_lines[j1:j2]), added=True))
    return parts


def generate_diff_string(old_content: str, new_content: str, context_lines: int = 4) -> DisplayDiff:
    """Generate a display-oriented diff with line numbers and context."""

    parts = _parts(old_content, new_content)
    output: list[str] = []
    old_lines = old_content.split("\n")
    new_lines = new_content.split("\n")
    line_number_width = len(str(max(len(old_lines), len(new_lines))))
    old_line_number = 1
    new_line_number = 1
    last_was_change = False
    first_changed_line: int | None = None

    for index, part in enumerate(parts):
        raw = part.value.split("\n")
        if raw and raw[-1] == "":
            raw.pop()

        if part.added or part.removed:
            if first_changed_line is None:
                first_changed_line = new_line_number
            for line in raw:
                if part.added:
                    number = str(new_line_number).rjust(line_number_width)
                    output.append(f"+{number} {line}")
                    new_line_number += 1
                else:
                    number = str(old_line_number).rjust(line_number_width)
                    output.append(f"-{number} {line}")
                    old_line_number += 1
            last_was_change = True
            continue

        next_is_change = index < len(parts) - 1 and (parts[index + 1].added or parts[index + 1].removed)
        if last_was_change and next_is_change:
            if len(raw) <= context_lines * 2:
                for line in raw:
                    number = str(old_line_number).rjust(line_number_width)
                    output.append(f" {number} {line}")
                    old_line_number += 1
                    new_line_number += 1
            else:
                leading = raw[:context_lines]
                trailing = raw[-context_lines:]
                skipped = len(raw) - len(leading) - len(trailing)
                for line in leading:
                    number = str(old_line_number).rjust(line_number_width)
                    output.append(f" {number} {line}")
                    old_line_number += 1
                    new_line_number += 1
                output.append(f" {' ' * line_number_width} ...")
                old_line_number += skipped
                new_line_number += skipped
                for line in trailing:
                    number = str(old_line_number).rjust(line_number_width)
                    output.append(f" {number} {line}")
                    old_line_number += 1
                    new_line_number += 1
        elif last_was_change:
            shown = raw[:context_lines]
            skipped = len(raw) - len(shown)
            for line in shown:
                number = str(old_line_number).rjust(line_number_width)
                output.append(f" {number} {line}")
                old_line_number += 1
                new_line_number += 1
            if skipped > 0:
                output.append(f" {' ' * line_number_width} ...")
                old_line_number += skipped
                new_line_number += skipped
        elif next_is_change:
            skipped = max(0, len(raw) - context_lines)
            if skipped > 0:
                output.append(f" {' ' * line_number_width} ...")
                old_line_number += skipped
                new_line_number += skipped
            for line in raw[skipped:]:
                number = str(old_line_number).rjust(line_number_width)
                output.append(f" {number} {line}")
                old_line_number += 1
                new_line_number += 1
        else:
            old_line_number += len(raw)
            new_line_number += len(raw)
        last_was_change = False

    return DisplayDiff("\n".join(output), first_changed_line)


__all__ = [
    "AppliedEdits",
    "DisplayDiff",
    "FuzzyMatchResult",
    "MatchedEdit",
    "apply_edits_to_normalized_content",
    "apply_replacements_preserving_unchanged_lines",
    "detect_line_ending",
    "fuzzy_find_text",
    "generate_diff_string",
    "generate_unified_patch",
    "normalize_for_fuzzy_match",
    "normalize_to_lf",
    "restore_line_endings",
    "strip_bom",
]
