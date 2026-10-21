"""Continuation state across an ingest batch boundary (#227).

An ingester continues by handing state to ``ctx.set_session``; the server
persists it and hands it back on the next pull. That session is the *only*
continuation mechanism — the position-style ``offset`` parameter that used to
sit beside it was never populated and has been removed.

Which makes non-progress decidable. The inputs that vary between batches of a
run are the session and the batch limit, so an ingester that hands back the
state it was given will be handed it again: the next pull is byte-identical and
the run cannot advance. :class:`Continuation` is the cheap half of that test
and :class:`BatchWindow` the confirming half — an ingester whose session is a
constant marker but whose records genuinely differ (a draining queue) is
progressing, and must not be mistaken for a loop.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from osa.domain.ingest.model.ingester_record import IngesterRecord

_NO_SESSION = "-"
_FINGERPRINT_CHARS = 8


@dataclass(frozen=True)
class Continuation:
    """The session state an ingester was given, and the one it handed back."""

    before: dict[str, Any] | None
    after: dict[str, Any] | None

    @property
    def unchanged(self) -> bool:
        """True when the ingester handed back exactly the state it was given.

        False whenever no state was handed back (``after is None`` ends the
        run) or none was handed in (the first batch of a run).
        """
        return self.after is not None and self.before == self.after

    @property
    def fingerprint(self) -> str:
        """Short stable digest of the outgoing session, for the per-batch log line.

        Key order never changes the digest, so two batches that continue from
        the same state read identically in the log. ``"-"`` when the batch set
        no session. Never raises: a fingerprint must not be the thing that
        fails a batch, so unserialisable values fall back to ``repr``.
        """
        if self.after is None:
            return _NO_SESSION
        canonical = json.dumps(self.after, sort_keys=True, default=repr)
        return hashlib.sha256(canonical.encode()).hexdigest()[:_FINGERPRINT_CHARS]


@dataclass(frozen=True)
class BatchWindow:
    """The set of upstream ids one batch emitted."""

    source_ids: frozenset[str]

    @classmethod
    def of(cls, records: Sequence[IngesterRecord]) -> BatchWindow:
        return cls(source_ids=frozenset(record.source_id for record in records))

    def repeats(self, previous: BatchWindow) -> bool:
        """True when this batch re-emitted exactly the previous batch's window.

        An empty window never repeats: a batch that pulled nothing ends the run
        through ``has_more``, which is progress, not a stall.
        """
        return bool(self.source_ids) and self.source_ids == previous.source_ids
