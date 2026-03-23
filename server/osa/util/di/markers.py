"""Shared Dishka markers for conditional DI activation."""

from dishka import Marker

K8S = Marker("k8s")
"""Activates when runner.backend == "k8s" — enables K8s runners and S3 storage."""
