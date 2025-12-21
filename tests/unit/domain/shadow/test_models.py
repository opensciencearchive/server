from datetime import datetime
from osa.domain.shadow.model.aggregate import ShadowRequest, ShadowId
from osa.domain.shadow.model.report import ShadowReport
from osa.domain.shadow.model.value import ShadowStatus
from osa.domain.shared.model.srn import DepositionProfileSRN

class TestShadowModels:
    def test_shadow_request_creation(self):
        req = ShadowRequest(
            id=ShadowId("shadow-123"),
            status=ShadowStatus.PENDING,
            source_url="http://example.com/data.zip",
            profile_srn=DepositionProfileSRN.parse("urn:osa:osa-registry:profile:default@1.0.0")
        )
        
        assert req.id == "shadow-123"
        assert req.status == ShadowStatus.PENDING
        assert req.source_url == "http://example.com/data.zip"
        assert req.deposition_id is None

    def test_shadow_report_creation(self):
        report = ShadowReport(
            shadow_id=ShadowId("shadow-123"),
            source_domain="example.com",
            validation_summary={"status": "pass"},
            score="5/5",
            created_at=datetime.now()
        )
        
        assert report.shadow_id == "shadow-123"
        assert report.source_domain == "example.com"
        assert report.score == "5/5"
