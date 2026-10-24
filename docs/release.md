# Releasing

The port releases in lockstep with upstream: `pi-durable-python` X.Y.Z translates
`@earendil-works/pi-durable` X.Y.Z. Syncing a new upstream release is a version bump, not a routine
merge — vendor the release's specification, port the delta, then publish under the new number.

## Before the release commit

1. **Parity is green.** `PARITY.md` has no `planned` or `code` rows, and
   [`parity-report.md`](./parity-report.md) names every translated suite and each remaining
   deviation.

2. **The version agrees, and only where it belongs.** The upstream tag and commit are pinned in
   [`docs/upstream/README.md`](./upstream/README.md), `PARITY.md`, and the
   [ADR](./adr/0001-parity-baseline-and-boundary.md); the number is carried by:
   - `pyproject.toml` → `[project] version`
   - `src/pi_durable/__init__.py` → `__version__`

   `tests/test_scaffold.py` checks `__version__` against the vendored upstream version, and
   `tests/test_release_docs.py` checks that the packaging metadata agrees with it, that every extra
   is documented, and that the docs never re-pin the baseline. The CI `package` job compares the
   installed artifact's `__version__` to `pyproject.toml`, so a bump that misses either file fails
   before tagging. The README and usage guide name no version: the same-numbered rule plus the
   pinned baseline is all a reader needs.

3. **Lint, types, and tests pass from a clean environment.**

   ```console
   uv sync --extra dev,openai,anthropic,schema
   uv run ruff check src tests
   uv run mypy
   uv run pytest
   ```

4. **The docs describe this release.** The README's install and extras tables match
   `[project.optional-dependencies]`, every example marked `<!-- example -->` runs in the suite, and
   `docs/release-notes-<version>.md` exists with notes for this number.

## Build and verify

```console
version="$(python -c "import tomllib; print(tomllib.load(open('pyproject.toml', 'rb'))['project']['version'])")"
rm -rf dist
uv build                     # or: python -m build
python -m pip install --upgrade build twine   # once, in a scratch environment
python -m twine check dist/*
```

Hatchling builds the wheel from `src/pi_durable` and the sdist from the repository files, so there is
no manifest to maintain. Verify the artifacts in fresh environments rather than the development one.
Both must import, expose the version, and carry the typing marker:

```console
python -m venv /tmp/pi-durable-wheel
/tmp/pi-durable-wheel/bin/python -m pip install dist/*.whl
/tmp/pi-durable-wheel/bin/python -c "import tomllib, pi_durable; expected = tomllib.load(open('pyproject.toml', 'rb'))['project']['version']; assert pi_durable.__version__ == expected, (pi_durable.__version__, expected)"
test -f /tmp/pi-durable-wheel/lib/python*/site-packages/pi_durable/py.typed

python -m venv /tmp/pi-durable-sdist
/tmp/pi-durable-sdist/bin/python -m pip install dist/*.tar.gz
/tmp/pi-durable-sdist/bin/python -c "import pi_durable; print(pi_durable.__version__)"
```

Then prove the extras resolve and stay independent. Each provider adapter must import without the
other SDK installed:

```console
wheel="$(ls "$PWD"/dist/*.whl)"
for extra in openai anthropic schema all dev; do
  python -m venv "/tmp/pi-durable-$extra"
  "/tmp/pi-durable-$extra/bin/python" -m pip install "pi-durable-python[$extra] @ file://$wheel"
done

/tmp/pi-durable-openai/bin/python -c "import pi_durable.providers.openai, importlib.util; assert importlib.util.find_spec('anthropic') is None"
/tmp/pi-durable-anthropic/bin/python -c "import pi_durable.providers.anthropic, importlib.util; assert importlib.util.find_spec('openai') is None"
/tmp/pi-durable-schema/bin/python -c "import jsonschema"
/tmp/pi-durable-dev/bin/python -c "import pytest, ruff, mypy"
```

CI's `package` job builds both artifacts, runs `twine check`, and installs each in a fresh
environment on every push; the `extras` job installs each provider extra alone. This section is what
to do locally before tagging.

## Tag and publish

The tag is `v<version>`, matching upstream's own tag format.

```console
version="$(python -c "import tomllib; print(tomllib.load(open('pyproject.toml', 'rb'))['project']['version'])")"
git tag -a "v$version" -m "pi-durable-python $version (parity with @earendil-works/pi-durable $version)"
git push origin main
git push origin "v$version"

python -m twine upload dist/*           # reads ~/.pypirc
# or: uv publish --token "$UV_PUBLISH_TOKEN" dist/*   (uv does not read ~/.pypirc)
```

Publish only artifacts built from the tagged commit. The version is immutable once PyPI accepts it;
a mistake can be yanked, not deleted.

Create the GitHub release from the tag, with the notes for this version:

```console
gh release create "v$version" dist/* --title "$version" --notes-file "docs/release-notes-$version.md"
```

## After publishing

1. Confirm the PyPI page shows the new version with the expected extras and the rendered README:

   ```console
   curl -s "https://pypi.org/pypi/pi-durable-python/$version/json" \
     | python -c "import json, sys; info = json.load(sys.stdin)['info']; core = [req for req in (info['requires_dist'] or []) if 'extra ==' not in req]; print(info['version'], sorted(info['provides_extra'] or []), 'core dependencies:', core or 'none')"
   ```

2. Install the published package once from PyPI, not from `dist/`, to catch an upload that dropped
   files:

   ```console
   python -m venv /tmp/pi-durable-pypi
   /tmp/pi-durable-pypi/bin/python -m pip install "pi-durable-python==$version"
   /tmp/pi-durable-pypi/bin/python -c "import pi_durable; print(pi_durable.__version__)"
   ```

3. Close the release ticket with a receipt: the commit sha, the source and test paths the release
   touched (`README.md`, `docs/usage.md`, `docs/release.md`, `docs/release-notes-<version>.md`,
   `pyproject.toml`, `.github/workflows/ci.yml`, `tests/test_release_docs.py`,
   `tests/test_scaffold.py`), and the acceptance boxes ticked.

## A Python-only patch

If the port needs a fix before upstream moves, the version becomes `<version>.post1` and `PARITY.md`
records the deviation. The lockstep promise covers the semantics of the matching upstream release,
not the identity of the version string; a patch release never changes persisted shapes.
