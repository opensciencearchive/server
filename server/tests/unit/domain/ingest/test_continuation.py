"""Continuation / BatchWindow — the #227 non-progress signal, as pure values.

An ingester continues by handing the server state via ``ctx.set_session``; the
server hands that state back on the next pull. If the state it hands back is
the state it was given *and* the batch repeats the previous window, the next
pull is provably identical and the run cannot advance (#227).
"""

from osa.domain.ingest.model.continuation import BatchWindow, Continuation
from osa.domain.ingest.model.ingester_record import IngesterRecord


def _records(*source_ids: str) -> list[IngesterRecord]:
    return [IngesterRecord(source_id=sid, metadata={}) for sid in source_ids]


class TestContinuationUnchanged:
    def test_first_batch_is_never_unchanged(self) -> None:
        # Nothing was handed in, so nothing can have been handed back.
        assert Continuation(before=None, after={"offset": 1000}).unchanged is False

    def test_handing_back_the_same_state_is_unchanged(self) -> None:
        # The #227 signature: pull() slices by a phantom offset, so the session
        # it computes is identical every batch.
        state = {"offset": 1000, "total": 7600}
        assert Continuation(before=dict(state), after=dict(state)).unchanged is True

    def test_advancing_state_is_changed(self) -> None:
        assert Continuation(before={"offset": 0}, after={"offset": 1000}).unchanged is False

    def test_final_batch_is_never_unchanged(self) -> None:
        # Not setting a session is what ends the run — never a stall.
        assert Continuation(before={"offset": 7000}, after=None).unchanged is False

    def test_no_session_on_either_side_is_not_unchanged(self) -> None:
        assert Continuation(before=None, after=None).unchanged is False

    def test_partial_change_is_changed(self) -> None:
        before = {"offset": 1000, "total": 7600}
        after = {"offset": 1000, "total": 7601}
        assert Continuation(before=before, after=after).unchanged is False


class TestContinuationFingerprint:
    def test_fingerprint_is_stable_across_key_order(self) -> None:
        a = Continuation(before=None, after={"a": 1, "b": 2}).fingerprint
        b = Continuation(before=None, after={"b": 2, "a": 1}).fingerprint
        assert a == b

    def test_fingerprint_differs_for_different_state(self) -> None:
        a = Continuation(before=None, after={"offset": 1000}).fingerprint
        b = Continuation(before=None, after={"offset": 2000}).fingerprint
        assert a != b

    def test_fingerprint_of_no_session(self) -> None:
        assert Continuation(before={"offset": 1}, after=None).fingerprint == "-"

    def test_fingerprint_is_short(self) -> None:
        # It rides a per-batch log line, so it has to stay glanceable.
        assert len(Continuation(before=None, after={"offset": 1000}).fingerprint) == 8

    def test_fingerprint_survives_unserialisable_state(self) -> None:
        # Sessions arrive as JSON, but a fingerprint must never be the thing
        # that fails a batch.
        assert Continuation(before=None, after={"when": object()}).fingerprint != "-"


class TestBatchWindow:
    def test_window_repeats_on_identical_source_ids(self) -> None:
        window = BatchWindow.of(_records("a", "b", "c"))
        assert window.repeats(BatchWindow.of(_records("c", "b", "a"))) is True

    def test_window_does_not_repeat_on_different_ids(self) -> None:
        window = BatchWindow.of(_records("d", "e"))
        assert window.repeats(BatchWindow.of(_records("a", "b"))) is False

    def test_window_does_not_repeat_on_partial_overlap(self) -> None:
        window = BatchWindow.of(_records("b", "c"))
        assert window.repeats(BatchWindow.of(_records("a", "b"))) is False

    def test_empty_window_never_repeats(self) -> None:
        # A batch that pulled nothing ends the run on has_more; it is not a stall.
        assert BatchWindow.of([]).repeats(BatchWindow.of([])) is False

    def test_window_ignores_duplicate_ids_within_a_batch(self) -> None:
        assert BatchWindow.of(_records("a", "a", "b")) == BatchWindow.of(_records("a", "b"))
