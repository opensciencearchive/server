"""Reject exception for hooks that reject depositions."""


class Reject(Exception):
    """Raised by a @hook function to reject a deposition."""
