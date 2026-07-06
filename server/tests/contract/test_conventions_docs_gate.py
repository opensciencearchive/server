"""DB-free contract tests for the mandatory-docs deploy gate (#151, US2).

``POST /api/v1/conventions`` rejects any payload without a complete ``docs``
block with a 422 whose field-level errors name each gap — regardless of client
(FR-015/016). Body validation runs at the route layer, before auth/DI, so
these tests need no database and no credentials.

Persistence round-trip lives in
``tests/integration/test_convention_docs_postgres.py``.
"""

import os
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from osa.domain.deposition.command.create_convention import DeployConvention

os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")
os.environ.setdefault("OSA_AUTH__JWT__SECRET", "test-secret-for-contract-tests-minimum-32-chars")


def _client() -> AsyncClient:
    from osa.application.api.rest.app import create_app

    return AsyncClient(transport=ASGITransport(app=create_app()), base_url="http://test")


def _docs_block() -> dict[str, Any]:
    return {
        "purpose": "Test-campaign results for structural alloys.",
        "example_questions": [
            "Which alloys stay ductile below -40C?",
            "What is the yield strength range for 7xxx-series samples?",
            "Which samples have corrosion panels attached?",
        ],
        "examples": [
            {
                "question": "Which alloys stay ductile below -40C?",
                "query": 'POST /api/v1/data/alloy-tests/records {"filter": {}}',
                "interpretation": "Rows are individual test coupons.",
            }
        ],
    }


def _payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "title": "Alloy Ductility Tests",
        "description": "Mechanical test results for structural alloys",
        "file_requirements": {
            "accepted_types": [".csv"],
            "min_count": 0,
            "max_count": 5,
            "max_file_size": 100_000_000,
        },
        "schema": {
            "id": "alloy-tests",
            "version": "2.1.0",
            "fields": [
                {
                    "name": "alloy",
                    "type": "text",
                    "required": True,
                    "cardinality": "exactly_one",
                }
            ],
        },
        "hooks": [],
        "docs": _docs_block(),
    }
    payload.update(overrides)
    return payload


async def _post(payload: dict[str, Any]):
    async with _client() as client:
        return await client.post("/api/v1/conventions", json=payload)


def _error_locs(resp) -> list[tuple]:
    return [tuple(err["loc"]) for err in resp.json()["detail"]]


@pytest.mark.asyncio
class TestDocsGate:
    async def test_missing_docs_block_rejected_naming_the_gap(self):
        payload = _payload()
        del payload["docs"]
        resp = await _post(payload)
        assert resp.status_code == 422
        assert ("body", "docs") in _error_locs(resp)

    async def test_empty_purpose_rejected_naming_the_field(self):
        docs = _docs_block()
        docs["purpose"] = "   "
        resp = await _post(_payload(docs=docs))
        assert resp.status_code == 422
        assert any(loc[:3] == ("body", "docs", "purpose") for loc in _error_locs(resp))

    async def test_too_few_distinct_trigger_questions_rejected(self):
        docs = _docs_block()
        # 1 trigger question + 1 worked example repeating it = 1 distinct.
        docs["example_questions"] = ["Which alloys stay ductile below -40C?"]
        resp = await _post(_payload(docs=docs))
        assert resp.status_code == 422
        locs = _error_locs(resp)
        assert any(loc[:2] == ("body", "docs") for loc in locs)
        detail = resp.json()["detail"]
        assert any("3" in err["msg"] for err in detail)

    async def test_zero_examples_rejected_naming_the_field(self):
        docs = _docs_block()
        docs["examples"] = []
        resp = await _post(_payload(docs=docs))
        assert resp.status_code == 422
        assert any(loc[:3] == ("body", "docs", "examples") for loc in _error_locs(resp))

    async def test_multiple_gaps_all_named(self):
        docs = _docs_block()
        docs["purpose"] = ""
        docs["examples"] = []
        resp = await _post(_payload(docs=docs))
        assert resp.status_code == 422
        locs = _error_locs(resp)
        assert any(loc[:3] == ("body", "docs", "purpose") for loc in locs)
        assert any(loc[:3] == ("body", "docs", "examples") for loc in locs)

    async def test_extra_fields_still_forbidden(self):
        resp = await _post(_payload(unexpected="boom"))
        assert resp.status_code == 422

    async def test_extra_fields_inside_docs_forbidden(self):
        docs = _docs_block()
        docs["unexpected"] = "boom"
        resp = await _post(_payload(docs=docs))
        assert resp.status_code == 422


class TestFullyDocumentedPayloadValidates:
    def test_valid_payload_passes_dto_validation(self):
        # The full HTTP deploy round-trip (auth + DB) lives in the integration
        # suite; here we assert the DTO gate itself accepts a complete payload.
        cmd = DeployConvention.model_validate(_payload())
        assert cmd.docs.purpose.startswith("Test-campaign")
        assert len(cmd.docs.examples) == 1

    def test_worked_example_questions_count_toward_three(self):
        docs = _docs_block()
        docs["example_questions"] = ["q1?", "q2?"]
        docs["examples"][0]["question"] = "q3?"
        cmd = DeployConvention.model_validate(_payload(docs=docs))
        assert len(cmd.docs.example_questions) == 2
