"""Tests for K8s API error classification."""

from osa.domain.shared.error import PermanentError, TransientError
from osa.infrastructure.k8s.errors import classify_api_error


class _FakeApiException(Exception):
    """Stand-in for kubernetes_asyncio.client.ApiException."""

    def __init__(self, status: int, reason: str = ""):
        self.status = status
        self.reason = reason
        super().__init__(f"{status}: {reason}")


class TestClassifyApiError:
    def test_403_returns_permanent_runner_error(self):
        exc = _FakeApiException(403, "Forbidden")
        result = classify_api_error(exc)
        assert isinstance(result, PermanentError)
        assert "RBAC" in result.message or "permission" in result.message.lower()

    def test_404_returns_permanent_runner_error(self):
        exc = _FakeApiException(404, "Not Found")
        result = classify_api_error(exc)
        assert isinstance(result, PermanentError)

    def test_500_returns_transient_resource_error(self):
        exc = _FakeApiException(500, "Internal Server Error")
        result = classify_api_error(exc)
        assert isinstance(result, TransientError)

    def test_503_returns_transient_resource_error(self):
        exc = _FakeApiException(503, "Service Unavailable")
        result = classify_api_error(exc)
        assert isinstance(result, TransientError)

    def test_409_returns_transient_resource_error(self):
        exc = _FakeApiException(409, "Conflict")
        result = classify_api_error(exc)
        assert isinstance(result, TransientError)

    def test_unknown_status_returns_transient_resource_error(self):
        exc = _FakeApiException(429, "Too Many Requests")
        result = classify_api_error(exc)
        assert isinstance(result, TransientError)
