"""Tests for K8s naming utilities: Job names and label values."""

import re

from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN
from osa.infrastructure.k8s.naming import job_name, label_value, sanitize_label


class TestJobName:
    def test_basic_format(self):
        name = job_name("hook", "validate-dna", "urn:osa:localhost:dep:abc123")
        assert name.startswith("osa-hook-")
        assert "validate-dna" in name
        assert len(name) <= 63

    def test_dns_1035_compliant(self):
        """Output matches DNS-1035 label: lowercase alpha, digits, hyphens."""
        name = job_name("hook", "my_hook", "urn:osa:localhost:dep:test")
        assert re.match(r"^[a-z][a-z0-9-]*[a-z0-9]$", name), f"Invalid DNS-1035: {name}"

    def test_colons_replaced(self):
        name = job_name("hook", "validate", "urn:osa:archive.org:dep:abc123")
        assert ":" not in name

    def test_long_names_truncated_to_63(self):
        long_hook = "a" * 100
        long_srn = "urn:osa:very-long-domain.example.com:dep:" + "b" * 100
        name = job_name("hook", long_hook, long_srn)
        assert len(name) <= 63

    def test_random_suffix_for_uniqueness(self):
        name1 = job_name("hook", "validate", "urn:osa:localhost:dep:abc")
        name2 = job_name("hook", "validate", "urn:osa:localhost:dep:abc")
        # Names should differ due to random suffix
        assert name1 != name2

    def test_source_prefix(self):
        name = job_name("source", "geo-entrez", "urn:osa:localhost:dep:abc123")
        assert name.startswith("osa-source-")

    def test_unicode_stripped(self):
        name = job_name("hook", "validat\u00e9", "urn:osa:localhost:dep:abc")
        assert re.match(r"^[a-z][a-z0-9-]*[a-z0-9]$", name)

    def test_no_trailing_hyphen(self):
        name = job_name("hook", "test", "urn:osa:localhost:dep:abc")
        assert not name.endswith("-")

    def test_no_leading_digit(self):
        """DNS-1035 labels must start with a letter."""
        name = job_name("hook", "123test", "urn:osa:localhost:dep:abc")
        assert name[0].isalpha()


class TestSanitizeLabel:
    def test_replaces_colons(self):
        assert ":" not in sanitize_label("sha256:abc123def")

    def test_preserves_valid_chars(self):
        assert sanitize_label("hello-world_1.0") == "hello-world_1.0"

    def test_truncates_to_63(self):
        assert len(sanitize_label("a" * 100)) <= 63

    def test_strips_edge_chars(self):
        result = sanitize_label(".leading-and-trailing.")
        assert not result.startswith(".")
        assert not result.endswith(".")

    def test_collapses_runs(self):
        assert ".." not in sanitize_label("a::b")


class TestLabelValue:
    def test_deposition_srn(self):
        srn = DepositionSRN.parse("urn:osa:localhost:dep:abc123")
        result = label_value(srn)
        assert result == "localhost.dep.abc123"
        assert ":" not in result

    def test_convention_srn_with_version(self):
        srn = ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")
        result = label_value(srn)
        assert result == "localhost.conv.test.1.0.0"

    def test_no_colons_in_output(self):
        srn = DepositionSRN.parse("urn:osa:archive.university.edu:dep:xyz789")
        result = label_value(srn)
        assert ":" not in result
        assert re.match(r"^[a-zA-Z0-9._-]+$", result)

    def test_max_63_chars(self):
        long_id = "a" * 60  # LocalId max is 64; with "localhost.dep." prefix this exceeds 63
        srn = DepositionSRN.parse(f"urn:osa:localhost:dep:{long_id}")
        assert len(label_value(srn)) <= 63
