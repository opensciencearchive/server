"""Unit tests for RefreshToken entity."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4


from osa.domain.auth.model.token import RefreshToken
from osa.domain.auth.model.value import RefreshTokenId, TokenFamilyId, UserId


class TestRefreshTokenCreate:
    """Tests for RefreshToken.create factory method."""

    def test_create_sets_all_fields(self):
        """create should set all required fields."""
        user_id = UserId(uuid4())
        token_hash = "a" * 64
        family_id = TokenFamilyId(uuid4())

        token = RefreshToken.create(
            user_id=user_id,
            token_hash=token_hash,
            family_id=family_id,
            expires_in_days=7,
        )

        assert token.user_id == user_id
        assert token.token_hash == token_hash
        assert token.family_id == family_id
        assert token.revoked_at is None

    def test_create_generates_id(self):
        """create should generate a unique ID."""
        user_id = UserId(uuid4())

        token1 = RefreshToken.create(user_id, "a" * 64, TokenFamilyId(uuid4()))
        token2 = RefreshToken.create(user_id, "b" * 64, TokenFamilyId(uuid4()))

        assert token1.id != token2.id

    def test_create_sets_expiry_in_future(self):
        """create should set expires_at in the future."""
        user_id = UserId(uuid4())
        now = datetime.now(UTC)

        token = RefreshToken.create(
            user_id=user_id,
            token_hash="a" * 64,
            family_id=TokenFamilyId(uuid4()),
            expires_in_days=7,
        )

        assert token.expires_at > now
        # Should be roughly 7 days from now (allowing small margin)
        expected = now + timedelta(days=7)
        assert abs((token.expires_at - expected).total_seconds()) < 1

    def test_create_sets_created_at(self):
        """create should set created_at to current time."""
        now = datetime.now(UTC)

        token = RefreshToken.create(
            user_id=UserId(uuid4()),
            token_hash="a" * 64,
            family_id=TokenFamilyId(uuid4()),
        )

        assert abs((token.created_at - now).total_seconds()) < 1


class TestRefreshTokenIsValid:
    """Tests for RefreshToken.is_valid property."""

    def make_token(
        self,
        expires_at: datetime | None = None,
        revoked_at: datetime | None = None,
    ) -> RefreshToken:
        """Create a token with specified expiry and revocation."""
        if expires_at is None:
            expires_at = datetime.now(UTC) + timedelta(days=7)

        return RefreshToken(
            id=RefreshTokenId(uuid4()),
            user_id=UserId(uuid4()),
            token_hash="a" * 64,
            family_id=TokenFamilyId(uuid4()),
            expires_at=expires_at,
            created_at=datetime.now(UTC),
            revoked_at=revoked_at,
        )

    def test_is_valid_true_for_fresh_token(self):
        """is_valid should be True for non-expired, non-revoked token."""
        token = self.make_token()

        assert token.is_valid is True

    def test_is_valid_false_when_expired(self):
        """is_valid should be False when token is expired."""
        expired_at = datetime.now(UTC) - timedelta(hours=1)
        token = self.make_token(expires_at=expired_at)

        assert token.is_valid is False

    def test_is_valid_false_when_revoked(self):
        """is_valid should be False when token is revoked."""
        token = self.make_token(revoked_at=datetime.now(UTC))

        assert token.is_valid is False

    def test_is_valid_false_when_both_expired_and_revoked(self):
        """is_valid should be False when both expired and revoked."""
        token = self.make_token(
            expires_at=datetime.now(UTC) - timedelta(hours=1),
            revoked_at=datetime.now(UTC) - timedelta(hours=2),
        )

        assert token.is_valid is False


class TestRefreshTokenIsRevoked:
    """Tests for RefreshToken.is_revoked property."""

    def test_is_revoked_false_initially(self):
        """is_revoked should be False when revoked_at is None."""
        token = RefreshToken.create(
            user_id=UserId(uuid4()),
            token_hash="a" * 64,
            family_id=TokenFamilyId(uuid4()),
        )

        assert token.is_revoked is False

    def test_is_revoked_true_when_set(self):
        """is_revoked should be True when revoked_at is set."""
        token = RefreshToken(
            id=RefreshTokenId(uuid4()),
            user_id=UserId(uuid4()),
            token_hash="a" * 64,
            family_id=TokenFamilyId(uuid4()),
            expires_at=datetime.now(UTC) + timedelta(days=7),
            created_at=datetime.now(UTC),
            revoked_at=datetime.now(UTC),
        )

        assert token.is_revoked is True


class TestRefreshTokenIsExpired:
    """Tests for RefreshToken.is_expired property."""

    def test_is_expired_false_for_future_expiry(self):
        """is_expired should be False when expires_at is in the future."""
        token = RefreshToken(
            id=RefreshTokenId(uuid4()),
            user_id=UserId(uuid4()),
            token_hash="a" * 64,
            family_id=TokenFamilyId(uuid4()),
            expires_at=datetime.now(UTC) + timedelta(days=7),
            created_at=datetime.now(UTC),
            revoked_at=None,
        )

        assert token.is_expired is False

    def test_is_expired_true_for_past_expiry(self):
        """is_expired should be True when expires_at is in the past."""
        token = RefreshToken(
            id=RefreshTokenId(uuid4()),
            user_id=UserId(uuid4()),
            token_hash="a" * 64,
            family_id=TokenFamilyId(uuid4()),
            expires_at=datetime.now(UTC) - timedelta(hours=1),
            created_at=datetime.now(UTC) - timedelta(days=8),
            revoked_at=None,
        )

        assert token.is_expired is True


class TestRefreshTokenRevoke:
    """Tests for RefreshToken.revoke method."""

    def test_revoke_sets_revoked_at(self):
        """revoke should set revoked_at to current time."""
        token = RefreshToken.create(
            user_id=UserId(uuid4()),
            token_hash="a" * 64,
            family_id=TokenFamilyId(uuid4()),
        )
        assert token.revoked_at is None

        now = datetime.now(UTC)
        token.revoke()

        assert token.revoked_at is not None
        assert abs((token.revoked_at - now).total_seconds()) < 1

    def test_revoke_idempotent(self):
        """revoke should not change revoked_at if already revoked."""
        token = RefreshToken.create(
            user_id=UserId(uuid4()),
            token_hash="a" * 64,
            family_id=TokenFamilyId(uuid4()),
        )

        token.revoke()
        first_revoked_at = token.revoked_at

        # Second revoke should not change the timestamp
        token.revoke()

        assert token.revoked_at == first_revoked_at

    def test_revoke_makes_token_invalid(self):
        """revoke should make is_valid return False."""
        token = RefreshToken.create(
            user_id=UserId(uuid4()),
            token_hash="a" * 64,
            family_id=TokenFamilyId(uuid4()),
        )
        assert token.is_valid is True

        token.revoke()

        assert token.is_valid is False
