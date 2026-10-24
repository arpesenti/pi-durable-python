# Releasing

Releases are lockstep with upstream: the Python version tracks the `@earendil-works/pi-durable`
release it translates. Publishing 1.3.0 therefore means "this package implements upstream v1.3.0",
not "the third patch of our own line".

## Before the release commit

1. **Parity is green.** `PARITY.md` has no `planned` or `code` rows, and
   [`parity-report.md`](./parity-report.md) names every translated suite and each remaining deviation.
2. **The version agrees**:
   - `pyproject.toml` → `[project] version`
   - `src/pi_durable/__init__.py` → `__version__`
   - `README.md` → the baseline section; `docs/upstream/README.md` and `PARITY.md` carry the same
     upstream tag and commit.

   Two tests pin the contract: `tests/test_scaffold.py` holds the lockstep literal, and
   `tests/test_release_docs.py` checks that the packaging metadata agrees with `__version__` and that
   every extra is documented. The CI `package` job compares the installed artifact's `__version__`
   to `pyproject.toml`, so a bump that misses either file fails before tagging.
3. **Lint, types, and tests pass from a clean environment.**

   ```console
   uv sync --extra dev,openai,anthropic,schema
   uv run ruff check src tests
   uv run mypy
   uv run pytest
   ```

4. **The docs describe this release.** The README's install and extras tables match
   `[project.optional-dependencies]`, and every example marked `<!-- example -->` runs in the suite.

## Build and verify

```console
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
/tmp/pi-durable-wheel/bin/python -m pip install dist/pi_durable_python-1.3.0-py3-none-any.whl
/tmp/pi-durable-wheel/bin/python -c "import pi_durable; print(pi_durable.__version__)"
test -f /tmp/pi-durable-wheel/lib/python*/site-packages/pi_durable/py.typed

python -m venv /tmp/pi-durable-sdist
/tmp/pi-durable-sdist/bin/python -m pip install dist/pi_durable_python-1.3.0.tar.gz
/tmp/pi-durable-sdist/bin/python -c "import pi_durable; print(pi_durable.__version__)"
```

Then prove the extras resolve and stay independent. Each provider adapter must import without the
other SDK installed:

```console
wheel="$PWD/dist/pi_durable_python-1.3.0-py3-none-any.whl"
for extra in openai anthropic schema all dev; do
  python -m venv "/tmp/pi-durable-$extra"
  "/tmp/pi-durable-$extra/bin/python" -m pip install "pi-durable-python[$extra] @ file://$wheel"
done

/tmp/pi-durable-openai/bin/python -c "import pi_durable.providers.openai; import importlib.util; assert importlib.util.find_spec('anthropic') is None"
/tmp/pi-durable-anthropic/bin/python -c "import pi_durable.providers.anthropic; import importlib.util; assert importlib.util.find_spec('openai') is None"
/tmp/pi-durable-schema/bin/python -c "import jsonschema"
/tmp/pi-durable-dev/bin/python -c "import pytest, ruff, mypy"
```

CI's `package` job builds both artifacts, runs `twine check`, and installs each in a fresh
environment on every push; the `extras` job installs each provider extra alone. This section is what
to do locally before tagging.

## Tag and publish

The tag is `v<version>`, matching upstream's own tag format.

```console
git tag -a v1.3.0 -m "pi-durable-python 1.3.0 (parity with @earendil-works/pi-durable v1.3.0)"
git push origin main
git push origin v1.3.0

uv publish dist/*                       # reads ~/.pypirc or UV_PUBLISH_TOKEN
# or: python -m twine upload dist/*
```

Publish only artifacts built from the tagged commit. The version is immutable once PyPI accepts it;
a mistake can be yanked, not deleted.

Create the GitHub release from the tag, with the notes in
[`release-notes-1.3.0.md`](./release-notes-1.3.0.md):

```console
gh release create v1.3.0 dist/* --title "1.3.0" --notes-file docs/release-notes-1.3.0.md
```

## After publishing

1. Confirm the PyPI page shows 1.3.0 with the expected extras and the rendered README:

   ```console
   curl -s https://pypi.org/pypi/pi-durable-python/1.3.0/json \
     | python -c "import json, sys; info = json.load(sys.stdin)['info']; core = [req for req in (info['requires_dist'] or []) if 'extra ==' not in req]; print(info['version'], sorted(info['provides_extra'] or []), 'core dependencies:', core or 'none')"
   ```

2. Install the published package once from PyPI, not from `dist/`, to catch an upload that dropped
   files:

   ```console
   python -m venv /tmp/pi-durable-pypi
   /tmp/pi-durable-pypi/bin/python -m pip install "pi-durable-python==1.3.0"
   /tmp/pi-durable-pypi/bin/python -c "import pi_durable; print(pi_durable.__version__)"
   ```

3. Close the release ticket with a receipt: the commit sha, the source and test paths the release
   touched (`README.md`, `pyproject.toml`, `.github/workflows/ci.yml`, `docs/usage.md`,
   `docs/release.md`, `docs/release-notes-1.3.0.md`, `tests/test_release_docs.py`), and the acceptance
   boxes ticked.

## Deviating from upstream numbering

If the port ever needs a Python-only fix before upstream moves, the version becomes
`1.3.0.post1` and `PARITY.md` records the deviation. The lockstep promise covers the semantics of
v1.3.0, not the identity of the version string; a patch release never changes persisted shapes.
