"""Tests for K8s API error classification — facts only, no disposition (#152)."""

from osa.domain.shared.failure import FailureKind, RuntimeFailure
from osa.infrastructure.k8s.errors import classify_api_error


class _FakeApiException(Exception):
    """Stand-in for kubernetes_asyncio.client.ApiException."""

    def __init__(self, status: int, reason: str = ""):
        self.status = status
        self.reason = reason
        super().__init__(f"{status}: {reason}")


class TestClassifyApiError:
    def test_403_is_an_rbac_failure(self):
        exc = _FakeApiException(403, "Forbidden")
        result = classify_api_error(exc)
        assert isinstance(result, RuntimeFailure)
        assert result.kind is FailureKind.RBAC
        assert "RBAC" in result.detail or "permission" in result.detail.lower()

    def test_404_is_a_config_failure(self):
        exc = _FakeApiException(404, "Not Found")
        result = classify_api_error(exc)
        assert isinstance(result, RuntimeFailure)
        assert result.kind is FailureKind.CONFIG

    def test_500_is_a_runtime_failure(self):
        exc = _FakeApiException(500, "Internal Server Error")
        result = classify_api_error(exc)
        assert isinstance(result, RuntimeFailure)
        assert result.kind is FailureKind.RUNTIME

    def test_503_is_a_runtime_failure(self):
        exc = _FakeApiException(503, "Service Unavailable")
        result = classify_api_error(exc)
        assert result.kind is FailureKind.RUNTIME

    def test_409_is_a_runtime_failure(self):
        exc = _FakeApiException(409, "Conflict")
        result = classify_api_error(exc)
        assert result.kind is FailureKind.RUNTIME

    def test_unknown_status_is_a_runtime_failure(self):
        exc = _FakeApiException(429, "Too Many Requests")
        result = classify_api_error(exc)
        assert result.kind is FailureKind.RUNTIME
