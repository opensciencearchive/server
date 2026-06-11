"""Reserved names that collide with fixed URL slots under ``/data/``.

The unified ``/data/`` read surface exposes a small set of fixed path
segments (``/data/records/...``, and the deferred ``/data/datasets`` slot).
A schema ID or hook/feature name equal to one of these would shadow the
fixed route, so they are forbidden at aggregate construction time.

This module is the single source of truth for the reserved set. Adding a new
reserved slot (e.g. ``events``) later is a one-line change here.
"""

from __future__ import annotations

# Lowercase ASCII; only ever grows over the project's lifetime (shrinking
# would silently re-enable a name that a consumer's URL may already depend on).
RESERVED_NAMES: frozenset[str] = frozenset({"records", "datasets"})
