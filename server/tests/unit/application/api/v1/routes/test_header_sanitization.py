"""Tests for Content-Disposition header filename sanitization."""

from osa.application.api.v1.routes.depositions import _sanitize_header_filename


class TestSanitizeHeaderFilename:
    def test_normal_filename_unchanged(self):
        assert _sanitize_header_filename("data.csv") == "data.csv"

    def test_strips_double_quotes(self):
        assert _sanitize_header_filename('file"name.csv') == "file_name.csv"

    def test_strips_carriage_return(self):
        assert _sanitize_header_filename("file\rname.csv") == "file_name.csv"

    def test_strips_newline(self):
        assert _sanitize_header_filename("file\nname.csv") == "file_name.csv"

    def test_strips_crlf_injection(self):
        result = _sanitize_header_filename("file.csv\r\nX-Injected: true")
        assert "\r" not in result
        assert "\n" not in result
