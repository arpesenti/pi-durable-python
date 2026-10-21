"""Suite setup: ``PI_DURABLE_DOCUMENT_RETENTION_MS`` forces the default document retention.

The upstream suite loads ``setup-document-retention.ts`` for every test file so it can run with
each unattached document evicted right after use; ``python -m pytest`` with the variable set plays
the same part here.
"""

from __future__ import annotations

import os

from pi_durable.session.session import set_default_document_retention

_forced = os.environ.get("PI_DURABLE_DOCUMENT_RETENTION_MS")
if _forced is not None:
    set_default_document_retention(int(_forced))
