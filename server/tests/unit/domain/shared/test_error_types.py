"""Tests for worker delivery-control error types.

Container runners no longer raise these — they report RuntimeFailure facts
(see test_failure_policy.py). Transient/Permanent remain the worker's generic
retry / fail-now verbs for event handlers.
"""

from osa.domain.shared.error import (
    InfrastructureError,
    PermanentError,
    TransientError,
)


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
