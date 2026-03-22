"""Tests for RunnerConfig cross-field validation."""

import pytest
from pydantic import ValidationError

from osa.config import K8sConfig, RunnerConfig


class TestRunnerConfigValidation:
    """RunnerConfig validates required K8s fields when backend == 'k8s'."""

    def test_oci_backend_allows_empty_pvc(self):
        """OCI backend does not require K8s fields."""
        config = RunnerConfig(backend="oci")
        assert config.backend == "oci"

    def test_k8s_backend_requires_data_pvc_name(self):
        """K8s backend rejects empty data_pvc_name at config parse time."""
        with pytest.raises(ValidationError, match="data_pvc_name"):
            RunnerConfig(backend="k8s", k8s=K8sConfig(data_pvc_name=""))

    def test_k8s_backend_accepts_valid_pvc(self):
        """K8s backend accepts a non-empty data_pvc_name."""
        config = RunnerConfig(backend="k8s", k8s=K8sConfig(data_pvc_name="osa-data"))
        assert config.k8s.data_pvc_name == "osa-data"

    def test_k8s_backend_default_pvc_rejected(self):
        """K8s backend with default K8sConfig (empty pvc) is rejected."""
        with pytest.raises(ValidationError, match="data_pvc_name"):
            RunnerConfig(backend="k8s")
