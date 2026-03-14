"""Unit tests for UserCode value object."""

import pytest

from osa.domain.auth.model.value import UserCode


class TestUserCodeNormalization:
    """Tests for UserCode normalization on construction."""

    def test_strips_hyphens(self):
        """UserCode should strip hyphens during normalization."""
        code = UserCode("BCDF-2347")
        assert code.root == "BCDF2347"

    def test_strips_spaces(self):
        """UserCode should strip spaces during normalization."""
        code = UserCode("BCDF 2347")
        assert code.root == "BCDF2347"

    def test_uppercases(self):
        """UserCode should uppercase during normalization."""
        code = UserCode("bcdf2347")
        assert code.root == "BCDF2347"

    def test_combined_normalization(self):
        """UserCode should handle hyphens, spaces, and lowercase together."""
        code = UserCode("bcdf - 2347")
        assert code.root == "BCDF2347"

    def test_already_normalized(self):
        """UserCode should accept already-normalized codes."""
        code = UserCode("BCDF2347")
        assert code.root == "BCDF2347"


class TestUserCodeValidation:
    """Tests for UserCode validation."""

    def test_rejects_too_short(self):
        """UserCode should reject codes shorter than 8 chars."""
        with pytest.raises(ValueError, match="Invalid user code"):
            UserCode("BCDF234")

    def test_rejects_too_long(self):
        """UserCode should reject codes longer than 8 chars."""
        with pytest.raises(ValueError, match="Invalid user code"):
            UserCode("BCDF23478")

    def test_rejects_vowels(self):
        """UserCode should reject codes containing vowels."""
        with pytest.raises(ValueError, match="Invalid user code"):
            UserCode("ABCD2347")  # A is a vowel

    def test_rejects_ambiguous_chars(self):
        """UserCode should reject ambiguous characters (0, O, 1, I, 5)."""
        for bad_char in "0O1I5":
            with pytest.raises(ValueError, match="Invalid user code"):
                UserCode(f"BCDF234{bad_char}")

    def test_rejects_empty(self):
        """UserCode should reject empty strings."""
        with pytest.raises(ValueError, match="Invalid user code"):
            UserCode("")


class TestUserCodeDisplay:
    """Tests for UserCode display formatting."""

    def test_display_format(self):
        """UserCode.display should format as XXXX-XXXX."""
        code = UserCode("BCDF2347")
        assert code.display == "BCDF-2347"

    def test_display_from_hyphenated_input(self):
        """UserCode.display should work after normalization."""
        code = UserCode("bcdf-2347")
        assert code.display == "BCDF-2347"


class TestUserCodeEquality:
    """Tests for UserCode equality and hashing."""

    def test_equal_codes(self):
        """UserCodes with same value should be equal."""
        code1 = UserCode("BCDF2347")
        code2 = UserCode("bcdf-2347")
        assert code1 == code2

    def test_hashable(self):
        """UserCode should be usable as dict key / set member."""
        code = UserCode("BCDF2347")
        assert hash(code) == hash(UserCode("bcdf-2347"))
