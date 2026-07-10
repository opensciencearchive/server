"""Unit tests for the SubmissionStage progress checkpoint (#160)."""

from osa.domain.deposition.model.value import SubmissionStage


class TestSubmissionStageOrdering:
    def test_definition_order_is_total_order(self):
        assert SubmissionStage.SUBMITTED < SubmissionStage.VALIDATED
        assert SubmissionStage.VALIDATED < SubmissionStage.PUBLISHED
        assert SubmissionStage.SUBMITTED < SubmissionStage.PUBLISHED

    def test_greater_than_derived_by_total_ordering(self):
        assert SubmissionStage.PUBLISHED > SubmissionStage.SUBMITTED
        assert SubmissionStage.VALIDATED > SubmissionStage.SUBMITTED

    def test_less_than_or_equal(self):
        assert SubmissionStage.SUBMITTED <= SubmissionStage.SUBMITTED
        assert SubmissionStage.SUBMITTED <= SubmissionStage.VALIDATED

    def test_greater_than_or_equal(self):
        assert SubmissionStage.PUBLISHED >= SubmissionStage.PUBLISHED
        assert SubmissionStage.VALIDATED >= SubmissionStage.SUBMITTED

    def test_equal_member_is_not_less_than(self):
        assert not (SubmissionStage.VALIDATED < SubmissionStage.VALIDATED)
        assert not (SubmissionStage.VALIDATED > SubmissionStage.VALIDATED)

    def test_string_values(self):
        assert SubmissionStage.SUBMITTED == "submitted"
        assert SubmissionStage.VALIDATED == "validated"
        assert SubmissionStage.PUBLISHED == "published"

    def test_reconstruct_from_value(self):
        assert SubmissionStage("submitted") is SubmissionStage.SUBMITTED
        assert SubmissionStage("published") is SubmissionStage.PUBLISHED
