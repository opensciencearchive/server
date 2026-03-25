"""K8s naming utilities: Job names (DNS-1035) and label values."""

from __future__ import annotations

import re
import secrets
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from osa.domain.shared.model.srn import SRN


def sanitize_label(raw: str) -> str:
    """Sanitize a raw string for use as a K8s label value.

    K8s label values must match [a-zA-Z0-9._-], max 63 chars.
    Replaces invalid characters with dots and collapses runs.
    """
    sanitized = re.sub(r"[^a-zA-Z0-9._-]", ".", raw)
    sanitized = re.sub(r"[._-]{2,}", ".", sanitized)
    return sanitized[:63].strip("-._")


def label_value(value: str | SRN) -> str:
    """Convert a string or SRN to a K8s-safe label value.

    For SRN objects, strips the constant ``urn:osa:`` prefix to save space
    within the 63-char K8s label limit. For plain strings, sanitizes directly.

    Examples:
        label_value("run-abc123") → "run-abc123"
        label_value(DepositionSRN.parse("urn:osa:localhost:dep:abc123"))
        → "localhost.dep.abc123"
    """
    from osa.domain.shared.model.srn import SRN

    if isinstance(value, SRN):
        compact = f"{value.domain.root}.{value.type.value}.{value.id.root}"
        if value.version is not None:
            compact += f".{value.version}"
        return sanitize_label(compact)
    return sanitize_label(value)


def job_name(prefix: str, hook_name: str, deposition_srn: str) -> str:
    """Generate a K8s Job name from prefix, hook name, and deposition SRN.

    Output conforms to DNS-1035: lowercase alphanumeric + hyphens,
    starts with a letter, max 63 characters. A 4-char random suffix
    ensures uniqueness.

    Examples:
        job_name("hook", "validate-dna", "urn:osa:localhost:dep:abc123")
        → "osa-hook-validate-dna-abc123-x7k2"
    """
    suffix = secrets.token_hex(2)  # 4 hex chars

    # Extract the ID fragment from the SRN (last component)
    srn_parts = deposition_srn.split(":")
    dep_fragment = srn_parts[-1] if srn_parts else deposition_srn

    raw = f"osa-{prefix}-{hook_name}-{dep_fragment}-{suffix}"

    # Sanitize: lowercase, replace non-DNS chars with hyphens
    sanitized = raw.lower()
    sanitized = re.sub(r"[^a-z0-9-]", "-", sanitized)
    # Collapse multiple hyphens
    sanitized = re.sub(r"-+", "-", sanitized)
    # Strip leading/trailing hyphens
    sanitized = sanitized.strip("-")

    # Ensure starts with a letter
    if sanitized and not sanitized[0].isalpha():
        sanitized = "osa-" + sanitized

    # Truncate to 63 chars, strip trailing hyphen after truncation
    sanitized = sanitized[:63].rstrip("-")

    return sanitized
