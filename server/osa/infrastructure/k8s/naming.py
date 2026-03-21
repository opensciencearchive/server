"""SRN-to-Job-name sanitization for DNS-1035 compliance."""

import re
import secrets


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
