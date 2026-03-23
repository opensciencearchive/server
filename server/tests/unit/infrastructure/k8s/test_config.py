"""Tests for RunnerConfig cross-field validation."""

import pytest
from pydantic import ValidationError

from osa.config import K8sConfig, RunnerConfig


def _valid_k8s(**overrides: object) -> K8sConfig:
    """Build a valid K8sConfig with all required fields, allowing overrides."""
    defaults = {"data_pvc_name": "osa-data", "s3_bucket": "osa-data"}
    return K8sConfig(**(defaults | overrides))  # type: ignore[arg-type]


class TestRunnerConfigValidation:
    """RunnerConfig validates required K8s fields when backend == 'k8s'."""

    def test_oci_backend_allows_empty_pvc(self):
        """OCI backend does not require K8s fields."""
        config = RunnerConfig(backend="oci")
        assert config.backend == "oci"

    def test_k8s_backend_requires_data_pvc_name(self):
        """K8s backend rejects empty data_pvc_name at config parse time."""
        with pytest.raises(ValidationError, match="data_pvc_name"):
            RunnerConfig(backend="k8s", k8s=K8sConfig(data_pvc_name="", s3_bucket="b"))

    def test_k8s_backend_requires_s3_bucket(self):
        """K8s backend rejects empty s3_bucket at config parse time."""
        with pytest.raises(ValidationError, match="s3_bucket"):
            RunnerConfig(backend="k8s", k8s=K8sConfig(data_pvc_name="pvc", s3_bucket=""))

    def test_k8s_backend_accepts_valid_config(self):
        """K8s backend accepts non-empty data_pvc_name and s3_bucket."""
        config = RunnerConfig(backend="k8s", k8s=_valid_k8s())
        assert config.k8s.data_pvc_name == "osa-data"
        assert config.k8s.s3_bucket == "osa-data"

    def test_k8s_backend_default_config_rejected(self):
        """K8s backend with default K8sConfig (empty fields) is rejected."""
        with pytest.raises(ValidationError):
            RunnerConfig(backend="k8s")
