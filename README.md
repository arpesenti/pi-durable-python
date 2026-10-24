# pi-durable-python

A Python port of [`@earendil-works/pi-durable`](https://github.com/earendil-works/pi/tree/main/packages/durable),
targeting **v1.3.0**.

> **Rewrite in progress.** `main` is being rebuilt from scratch against the v1.3.0 source and
> specification. There is no usable release yet. The previous architectural port is preserved at
> the `pre-rewrite` tag and in git history.

## Target

- Baseline: `@earendil-works/pi-durable` v1.3.0 (`71023aec`).
- Parity: semantics and persisted record shapes. Python names follow PEP 8.
- Runtime: CPython 3.11+ on `asyncio`; the suite runs on 3.11–3.15 and on the 3.14t/3.15t
  free-threaded builds with the GIL disabled. Concurrent threads sharing a `Session` are not a
  supported contract.
- Core: no runtime dependencies. Optional extras provide providers and JSON Schema validation.

## Tracking

- [`PARITY.md`](./PARITY.md) — one row per public export and spec section.
- [`docs/upstream/spec-1.3.0.md`](./docs/upstream/spec-1.3.0.md) — vendored normative spec.
- [`docs/adr/0001-parity-baseline-and-boundary.md`](./docs/adr/0001-parity-baseline-and-boundary.md) —
  why this is a rewrite, and what is in and out of scope.
- [`docs/parity-report.md`](./docs/parity-report.md) — how every in-scope v1.3.0 suite was
  translated, the audit fixes, and the remaining explicit deviations.

## Status

No installation or usage instructions yet. They return when the parity suite is green.

## License

MIT. The vendored v1.3.0 specification is MIT, copyright (c) 2025 Mario Zechner; see
[`docs/upstream/LICENSE`](./docs/upstream/LICENSE).
