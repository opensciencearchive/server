import pytest
from osa.domain.shared.model.srn import (
    SRN,
    RecordSRN,
    DepositionSRN,
    SchemaSRN,
    ResourceType,
)


class TestSRN:
    def test_parse_record_srn(self):
        raw = "urn:osa:node-1:rec:123@1"
        srn = RecordSRN.parse(raw)
        assert srn.type == ResourceType.rec
        assert srn.id.root == "123"
        assert srn.version is not None
        assert srn.version.root == 1

    def test_parse_schema_srn(self):
        raw = "urn:osa:node-1:schema:my-schema@1.0.0"
        srn = SchemaSRN.parse(raw)
        assert srn.type == ResourceType.schema
        assert srn.id.root == "my-schema"
        assert str(srn.version) == "1.0.0"

    def test_render_srn(self):
        srn = DepositionSRN.parse("urn:osa:node-1:dep:abc-123")
        assert str(srn) == "urn:osa:node-1:dep:abc-123"

    def test_invalid_srn(self):
        with pytest.raises(ValueError):
            SRN.parse("invalid:urn")
