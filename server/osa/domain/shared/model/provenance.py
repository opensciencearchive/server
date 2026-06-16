"""Per-row provenance reference carried through hook output storage (#145).

Design-revisions §6: ``run_id`` is carried to the feature-insert handlers via a
tiny ``run.json`` written into each hook's output dir (``…/hooks/{hook}/output/
run.json``), **not** reconstructed via a DB lookup. This is the parsed shape of
that file. ``run_id`` is the ``hook_runs.id`` stamped onto every feature row;
``release_id`` records which release produced it (redundant with the run, kept
for cheap debugging of orphaned output).
"""

from __future__ import annotations

from osa.domain.shared.model.value import ValueObject


class RunRef(ValueObject):
    """Contents of a hook output dir's ``run.json``."""

    run_id: str
    release_id: str
