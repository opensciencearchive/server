"""Tests for runner-specific error types."""

from osa.domain.shared.error import (
    InfrastructureError,
    OOMError,
    PermanentError,
    TransientError,
)


class TestOOMError:
    def test_is_infrastructure_error(self):
        err = OOMError("Hook killed by OOM")
        assert isinstance(err, InfrastructureError)

    def test_is_permanent_error(self):
        err = OOMError("Hook killed by OOM")
        assert isinstance(err, PermanentError)

    def test_message_and_code(self):
        err = OOMError("Hook killed by OOM")
        assert err.message == "Hook killed by OOM"
        assert err.code == "OOMError"


class TestTransientError:
    def test_is_infrastructure_error(self):
        err = TransientError("Pod scheduling timeout")
        assert isinstance(err, InfrastructureError)

    def test_message_and_code(self):
        err = TransientError("Pod scheduling timeout after 120s")
        assert err.message == "Pod scheduling timeout after 120s"
        assert err.code == "TransientError"


class TestPermanentError:
    def test_is_infrastructure_error(self):
        err = PermanentError("Image pull failed")
        assert isinstance(err, InfrastructureError)

    def test_message_and_code(self):
        err = PermanentError("Image pull failed: ImagePullBackOff")
        assert err.message == "Image pull failed: ImagePullBackOff"
        assert err.code == "PermanentError"
