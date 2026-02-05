"""Global test fixtures."""

import os

# Set JWT secret before any test modules import Config
# This must happen at module load time, not in a fixture
os.environ.setdefault("OSA_AUTH__JWT__SECRET", "test-secret-for-unit-tests-min-32")
