"""IngesterInputs carries only what the server actually populates (#227).

`offset` and `since` were both declared here, emitted as `OSA_OFFSET` /
`OSA_SINCE` by both runners, read by the SDK entrypoint and passed to `pull()`
— and never once set by the orchestrator. An ingester that paginated on
`offset` therefore looped forever (re-pulling window 0 while `has_more` stayed
True) or silently truncated after the first batch, depending on whether it also
set a session.

A typed, documented, permanently-empty parameter is the trap. This pins the
inputs to the fields the orchestrator builds, so the next continuation
mechanism has to be populated end to end before it can be advertised.
"""

from dataclasses import fields

from osa.domain.shared.port.ingester_runner import IngesterInputs

# Exactly what ProcessBatch._ingest constructs.
_POPULATED = {
    "convention_id",
    "ingest_run_id",
    "batch_index",
    "config",
    "limit",
    "session",
}


class TestIngesterInputsContract:
    def test_carries_no_unpopulated_fields(self) -> None:
        assert {f.name for f in fields(IngesterInputs)} == _POPULATED

    def test_has_no_offset(self) -> None:
        # Position-based continuation is gone: slicing by it is what looped.
        assert "offset" not in _POPULATED

    def test_has_no_since(self) -> None:
        # Incremental runs need a designed watermark (plus the scheduler that
        # feeds it), not a parameter that is always None.
        assert "since" not in _POPULATED

    def test_session_is_the_continuation_mechanism(self) -> None:
        assert "session" in {f.name for f in fields(IngesterInputs)}
