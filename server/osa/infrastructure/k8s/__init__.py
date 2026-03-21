"""Kubernetes runner infrastructure.

kubernetes-asyncio is an optional dependency. Modules that require it
(di.py, runner.py, source_runner.py, health.py) perform lazy imports
and raise ConfigurationError if the package is not installed.
"""
