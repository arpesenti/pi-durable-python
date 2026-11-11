# Upstream reference

Vendored material from `@earendil-works/pi-durable`, used as the normative reference for this port.

- Package: `@earendil-works/pi-durable`
- Version: `1.5.0`
- Tag: `v1.5.0`
- Commit: `f9a26365568127461c47a07a37dcb42c140a3ac5`
- Source: <https://github.com/earendil-works/pi/tree/v1.5.0/packages/durable>
- Retrieved: 2026-11-05
- License: MIT, copyright (c) 2025 Mario Zechner (see [`LICENSE`](./LICENSE))

## Files

- `spec-1.5.0.md` — vendored `docs/spec.md`; the normative reference. Do not edit.
- `group-commit-1.5.0.md` — vendored `docs/group-commit.md`; the group-commit contract behind spec
  section 4. Do not edit.
- `spec-1.4.0.md` — the previous baseline, kept as the audit record of the 1.4.0 port. Do not edit.
- `spec-1.3.0.md` — the previous baseline, kept as the audit record of the 1.3.0 port. Do not edit.
- `LICENSE` — upstream MIT license.

## Updating

Adopting a newer upstream release is a deliberate version bump, not a routine sync. Fetch the new
tag, vendor its `docs/spec.md` and its `docs/group-commit.md` under the release's version, each with
a provenance header naming the tag, the commit and the retrieval date recorded above, update this
file and the baseline in `PARITY.md`, then port the delta. Vendored files of earlier releases are the
audit record of those ports: add the new one, never rewrite an old one.
