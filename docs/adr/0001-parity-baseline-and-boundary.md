# Parity baseline and boundary for the Python port

**Status:** accepted

The previous Python port was an architectural adaptation, not a line-by-line translation, and had
grown deliberate deviations: non-streaming model access, no execution environment, no hooks or
compaction, and post-1.3.0 behavior mixed in. We are replacing it with a from-scratch mechanical
translation of `@earendil-works/pi-durable` **v1.3.0** (tag `v1.3.0`, commit
`71023aec1546d6d677a65669c5665c0927ce0dd0`) on `main`. Parity means matching semantics and persisted
record shapes; the `1.3.0` version number is a lockstep promise, not a label.

The public surface is the v1.3.0 root export plus the `env`, `tools`, `tool-search`, `testing`, and
storage subpaths, with OpenAI and Anthropic provider extras. Code Mode, the image implementation
(the `ImageProcessor` protocol remains), Cloudflare adapters, PowerShell, Windows support, and the
benchmark helpers are out of scope. Storage backends are ported faithfully; cross-language storage
interoperability is a bonus, not a contract. Python functions and parameters use PEP 8 names,
classes and types stay PascalCase, and persisted JSON keeps camelCase fields.

## Considered options

- **Evolve or hybrid-port the existing implementation.** Rejected: the current harness shape
  (non-streaming `complete()`, no `ExecutionEnv`, no hooks or compaction) would constrain the
  translation, and mixed old/new behavior would make the parity matrix untrustworthy.
- **Rewrite on a dedicated branch.** Rejected in favour of replacing `main` immediately. The old
  implementation remains recoverable at the annotated `pre-rewrite` tag and in git history.

## Consequences

- `main` is non-functional until the rewrite is complete; there is no partial release.
- The translated v1.3.0 test suite is the authority, not the previous 87 tests.
- `PARITY.md` records every export and spec section, including explicit exclusions.
- Storage interoperability must not be claimed or tested as a supported contract.
