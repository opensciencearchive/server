from datetime import datetime
from osa.domain.deposition.model.value import DepositionStatus, DepositionFile
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.shared.model.srn import DepositionSRN, DepositionProfileSRN

class TestDepositionAggregate:
    def test_create_deposition(self):
        srn = DepositionSRN.parse("urn:osa:mock-node:dep:mock-id")
        profile_srn = DepositionProfileSRN.parse("urn:osa:osa-registry:profile:default@1.0.0")
        
        dep = Deposition(
            srn=srn,
            profile_srn=profile_srn,
            status=DepositionStatus.DRAFT,
            payload={"title": "Test Deposition"}
        )
        
        assert dep.srn == srn
        assert dep.profile_srn == profile_srn
        assert dep.status == DepositionStatus.DRAFT
        assert dep.payload == {"title": "Test Deposition"}
        assert dep.files == []

    def test_add_and_remove_files(self):
        srn = DepositionSRN.parse("urn:osa:mock-node:dep:mock-id")
        profile_srn = DepositionProfileSRN.parse("urn:osa:osa-registry:profile:default@1.0.0")
        
        dep = Deposition(
            srn=srn,
            profile_srn=profile_srn,
            status=DepositionStatus.DRAFT,
            payload={}
        )
        
        file = DepositionFile(
            name="data.csv",
            size=1024,
            checksum="sha256:abc",
            uploaded_at=datetime.now()
        )
        
        dep.files.append(file)
        assert len(dep.files) == 1
        assert dep.files[0].name == "data.csv"
        
        dep.remove_all_files()
        assert len(dep.files) == 0
