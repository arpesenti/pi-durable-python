"""Translated v1.3.0 ``storage-runtime-boundary`` suite.

The portable subpaths must not pull in a host backend. Python's host-specific module for the
filesystem is ``pi_durable.env.posix``; the root export stays limited to portable core storage.
"""

from __future__ import annotations

import ast
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[1] / "src"
PACKAGE = "pi_durable"


def _module_path(parts: tuple[str, ...]) -> Path | None:
    module = SOURCE_ROOT.joinpath(*parts)
    if module.with_suffix(".py").is_file():
        return module.with_suffix(".py")
    if (module / "__init__.py").is_file():
        return module / "__init__.py"
    return None


def _package_parts(path: Path) -> tuple[str, ...]:
    relative = path.relative_to(SOURCE_ROOT)
    parts = list(relative.with_suffix("").parts)
    parts.pop()
    return tuple(parts)


def _resolve_import(path: Path, node: ast.ImportFrom | ast.Import) -> list[Path]:
    package = _package_parts(path)
    candidates: list[tuple[str, ...]] = []
    if isinstance(node, ast.Import):
        for alias in node.names:
            candidates.append(tuple(alias.name.split(".")))
    else:
        if node.level:
            base = list(package[: len(package) - (node.level - 1)])
        else:
            base = []
        if node.module:
            base.extend(node.module.split("."))
        if base:
            candidates.append(tuple(base))
        for alias in node.names:
            if base and alias.name != "*":
                candidates.append(tuple([*base, alias.name]))
    return [resolved for candidate in candidates if (resolved := _module_path(candidate))]


def _source_graph(*module: str) -> set[Path]:
    root = _module_path(module)
    assert root is not None, f"missing source entry {module}"
    visited: set[Path] = set()
    pending = [root]
    while pending:
        path = pending.pop()
        if path in visited:
            continue
        visited.add(path)
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                pending.extend(_resolve_import(path, node))
    return visited


def _relative_names(graph: set[Path]) -> set[str]:
    return {str(path.relative_to(SOURCE_ROOT)) for path in graph}


def test_keeps_the_package_root_limited_to_portable_core_storage() -> None:
    graph = _relative_names(_source_graph(PACKAGE))

    assert not any("/env/" in path for path in graph)
    assert not any("/storage/jsonl/" in path for path in graph)
    assert not any("/storage/sqlite/" in path for path in graph)


def test_keeps_the_portable_sqlite_subpath_free_of_a_host_adapter() -> None:
    graph = _relative_names(_source_graph(PACKAGE, "storage", "sqlite"))

    assert not any(path.endswith("/storage/sqlite/posix.py") for path in graph)
    assert not any(path.endswith("/storage/sqlite/node.py") for path in graph)
    assert not any(path.endswith("/storage/sqlite/cloudflare.py") for path in graph)


def test_keeps_the_portable_environment_subpath_free_of_the_posix_backend() -> None:
    graph = _relative_names(_source_graph(PACKAGE, "env"))

    assert not any(path.endswith("/env/posix.py") for path in graph)
    assert not any(path.endswith("/env/node.py") for path in graph)


def test_keeps_the_portable_jsonl_subpath_free_of_a_host_backend() -> None:
    graph = _relative_names(_source_graph(PACKAGE, "storage", "jsonl"))

    assert not any(path.endswith("/env/posix.py") for path in graph)
    assert not any(path.endswith("/env/node.py") for path in graph)
    assert not any(path.endswith("/storage/jsonl/posix.py") for path in graph)
    assert not any(path.endswith("/storage/jsonl/node.py") for path in graph)
