from osa.domain.shadow.model.aggregate import ShadowId, ShadowRequest
from osa.domain.shadow.model.value import ShadowStatus
from osa.domain.shared.model.srn import DepositionProfileSRN
from osa.infrastructure.persistence.mappers.shadow import shadow_request_to_dict, row_to_shadow_request

class TestShadowMappers:
    def test_shadow_request_mapping(self):
        profile_srn = DepositionProfileSRN.parse("urn:osa:osa-registry:profile:default@1.0.0")
        req = ShadowRequest(
            id=ShadowId("shadow-123"),
            status=ShadowStatus.PENDING,
            source_url="http://example.com",
            profile_srn=profile_srn
        )
        
        data = shadow_request_to_dict(req)
        assert data["id"] == "shadow-123"
        assert data["status"] == "pending"
        assert data["source_url"] == "http://example.com"
        assert data["profile_srn"] == str(profile_srn)
        
        reconstructed = row_to_shadow_request(data)
        assert reconstructed.id == req.id
        assert reconstructed.status == req.status
        assert str(reconstructed.profile_srn) == str(req.profile_srn)
