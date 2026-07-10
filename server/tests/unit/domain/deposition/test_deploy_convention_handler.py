"""Unit tests for DeployConventionHandler feature-table inlining (#160).

Feature-table creation used to be an event handler (``CreateFeatureTables``)
reacting to ``ConventionRegistered``. Once the event choreography collapsed
into orchestrated workflows, deploy inlines table creation at the command
handler: after ``ConventionService.deploy`` succeeds, one ``create_table`` per
declared hook, swallowing ``ConflictError`` so a re-deploy is idempotent. Cross-
domain composition happens here at the entry point — the deposition service must
not import the feature domain.
"""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.deposition.command.create_convention import (
    DeployConvention,
    DeployConventionHandler,
)
from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.docs import ConventionDocs, Example
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.shared.error import ConflictError
from osa.domain.shared.model.hook import HookName
from osa.domain.shared.model.srn import ConventionSlug, SchemaId


def _docs_payload() -> dict[str, Any]:
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


def _hook_payload(name: str) -> dict[str, Any]:
    return {
        "name": name,
        "feature": {
            "cardinality": "many",
            "columns": [{"name": "score", "json_type": "number", "required": True}],
        },
        "release": {
            "image": "ghcr.io/test/x",
            "digest": "sha256:abc",
            "config": {},
            "source_ref": "git:abc",
        },
    }


def _make_command(*hook_names: str) -> DeployConvention:
    payload: dict[str, Any] = {
        "title": "Alloy Ductility Tests",
        "description": "Mechanical test results for structural alloys",
        "file_requirements": {
            "accepted_types": [".csv"],
            "min_count": 0,
            "max_count": 5,
            "max_file_size": 100_000_000,
        },
        "schema": {"id": "alloy-tests", "version": "2.1.0", "fields": []},
        "hooks": [_hook_payload(n) for n in hook_names],
        "docs": _docs_payload(),
    }
    return DeployConvention.model_validate(payload)


def _make_convention(*hook_names: str) -> Convention:
    return Convention(
        id=ConventionSlug("alloy-ductility-tests"),
        title="Alloy Ductility Tests",
        description="Mechanical test results for structural alloys",
        schema_id=SchemaId.parse("alloy-tests@2.1.0"),
        file_requirements=FileRequirements(
            accepted_types=[".csv"], min_count=0, max_count=5, max_file_size=100_000_000
        ),
        hooks=[HookName(n) for n in hook_names],
        docs=ConventionDocs(
            purpose="p",
            example_questions=["a", "b", "c"],
            examples=[Example(question="a", query="q", interpretation="i")],
        ),
        created_at=datetime.now(UTC),
    )


def _make_handler(
    *,
    convention: Convention,
    deploy_error: Exception | None = None,
    create_table_error: Exception | None = None,
) -> DeployConventionHandler:
    convention_service = AsyncMock()
    if deploy_error is not None:
        convention_service.deploy.side_effect = deploy_error
    else:
        convention_service.deploy.return_value = convention

    feature_service = AsyncMock()
    if create_table_error is not None:
        feature_service.create_table.side_effect = create_table_error

    return DeployConventionHandler(
        principal=Principal(
            user_id=UserId(uuid4()),
            provider_identity=ProviderIdentity(provider="test", external_id="ext"),
            roles=frozenset(),
            scopes=frozenset({"conventions:write"}),
        ),
        convention_service=convention_service,
        feature_service=feature_service,
    )


class TestDeployInlinesFeatureTables:
    @pytest.mark.asyncio
    async def test_creates_a_table_for_every_hook_after_deploy(self):
        handler = _make_handler(convention=_make_convention("hook_a", "hook_b"))

        await handler.run(_make_command("hook_a", "hook_b"))

        assert handler.feature_service.create_table.await_count == 2
        created = {
            call.args[0].name.root for call in handler.feature_service.create_table.await_args_list
        }
        assert created == {"hook_a", "hook_b"}

    @pytest.mark.asyncio
    async def test_no_hooks_creates_no_tables(self):
        handler = _make_handler(convention=_make_convention())

        await handler.run(_make_command())

        handler.feature_service.create_table.assert_not_called()

    @pytest.mark.asyncio
    async def test_conflict_error_is_swallowed_for_idempotent_redeploy(self):
        # A re-deploy hits an already-existing table; ConflictError must not
        # propagate (mirrors the deleted CreateFeatureTables handler).
        handler = _make_handler(
            convention=_make_convention("hook_a"),
            create_table_error=ConflictError("table exists"),
        )

        # Must not raise.
        await handler.run(_make_command("hook_a"))

        handler.feature_service.create_table.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_non_conflict_error_from_create_table_propagates(self):
        handler = _make_handler(
            convention=_make_convention("hook_a"),
            create_table_error=RuntimeError("db down"),
        )

        with pytest.raises(RuntimeError, match="db down"):
            await handler.run(_make_command("hook_a"))

    @pytest.mark.asyncio
    async def test_tables_not_created_when_deploy_raises(self):
        handler = _make_handler(
            convention=_make_convention("hook_a"),
            deploy_error=RuntimeError("deploy failed"),
        )

        with pytest.raises(RuntimeError, match="deploy failed"):
            await handler.run(_make_command("hook_a"))

        handler.feature_service.create_table.assert_not_called()
