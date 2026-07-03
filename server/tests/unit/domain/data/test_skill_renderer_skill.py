"""SkillRenderer SKILL.md exact-output tests (#151, US1).

Pure renderer — string in/out, no I/O. The frontmatter name is
``osa-data-<sanitized-domain>`` (lowercase, non-[a-z0-9] → '-', runs
collapsed); the description joins the node description with the distinct
trigger-question union; the datasets table carries live counts and
root-relative reference links.
"""

from osa.domain.data.model.skill import AuthorDocs, DatasetEntry, ExampleDoc, NodeIdentity
from osa.domain.data.service.skill_renderer import SkillRenderer, sanitize_skill_name

BASE = "https://archive.university.edu"


def _node(**overrides: object) -> NodeIdentity:
    kwargs: dict = {
        "name": "Open Science Archive",
        "domain": "archive.university.edu",
        "description": "An open platform for depositing scientific data.",
        "osa_version": "0.0.5",
    }
    kwargs.update(overrides)
    return NodeIdentity.model_validate(kwargs)


def _docs() -> AuthorDocs:
    return AuthorDocs(
        purpose="Test-campaign results for structural alloys.",
        example_questions=[
            "Which alloys stay ductile below -40C?",
            "What is the yield strength range?",
        ],
        examples=[
            ExampleDoc(
                question="Which samples have corrosion panels attached?",
                query="GET /x",
                interpretation="means x",
            )
        ],
        when_not_to_use="Not a materials-property database.",
        see_also=["https://other-node.example.org"],
    )


def _dataset() -> DatasetEntry:
    return DatasetEntry(
        schema_id="alloy-tests",
        schema_ref="alloy-tests@2.1.0",
        title="Alloy Ductility Tests",
        row_count=12480,
    )


class TestSanitizeSkillName:
    def test_domain_dots_become_dashes(self) -> None:
        assert sanitize_skill_name("archive.university.edu") == "osa-data-archive-university-edu"

    def test_lowercases(self) -> None:
        assert sanitize_skill_name("ARCHIVE.University.EDU") == "osa-data-archive-university-edu"

    def test_collapses_runs(self) -> None:
        assert sanitize_skill_name("a..b__c") == "osa-data-a-b-c"

    def test_localhost(self) -> None:
        assert sanitize_skill_name("localhost") == "osa-data-localhost"


class TestRenderSkillExact:
    def test_full_document(self) -> None:
        renderer = SkillRenderer()
        out = renderer.render_skill(
            node=_node(),
            base_url=BASE,
            datasets=[_dataset()],
            docs=[_docs()],
        )
        expected = """\
---
name: osa-data-archive-university-edu
description: An open platform for depositing scientific data. Use when answering questions like: Which alloys stay ductile below -40C? What is the yield strength range? Which samples have corrosion panels attached?
---

# Open Science Archive

Test-campaign results for structural alloys.

## Datasets

| Schema | Title | Records | Reference |
|---|---|---|---|
| alloy-tests@2.1.0 | Alloy Ductility Tests | 12480 | api/v1/data/alloy-tests.md |

## Access

- Bulk:   GET  https://archive.university.edu/api/v1/data/<schema>/records.csv.gz
- Filter: POST https://archive.university.edu/api/v1/data/<schema>/records (FilterExpr body — use for large tables)
- One:    GET  https://archive.university.edu/api/v1/data/records/<id>

## When not to use

Not a materials-property database.

## See also

- https://other-node.example.org
"""
        assert out == expected


class TestRenderSkillParts:
    def test_description_without_docs_is_node_description_alone(self) -> None:
        out = SkillRenderer().render_skill(node=_node(), base_url=BASE, datasets=[], docs=[])
        assert "description: An open platform for depositing scientific data.\n" in out
        assert "Use when answering questions like" not in out

    def test_trigger_questions_are_distinct_across_sources(self) -> None:
        docs = AuthorDocs(
            purpose="p",
            example_questions=["q1?", "q2?"],
            examples=[ExampleDoc(question="q1?", query="x", interpretation="y")],
        )
        out = SkillRenderer().render_skill(
            node=_node(), base_url=BASE, datasets=[_dataset()], docs=[docs]
        )
        assert "questions like: q1? q2?\n" in out
        assert out.count("q1?") == 1

    def test_dataset_without_title_renders_empty_cell(self) -> None:
        ds = DatasetEntry(schema_id="orphan", schema_ref="orphan@1.0.0", title=None, row_count=0)
        out = SkillRenderer().render_skill(node=_node(), base_url=BASE, datasets=[ds], docs=[])
        assert "| orphan@1.0.0 |  | 0 | api/v1/data/orphan.md |" in out

    def test_when_not_to_use_omitted_without_author_content(self) -> None:
        out = SkillRenderer().render_skill(
            node=_node(), base_url=BASE, datasets=[_dataset()], docs=[]
        )
        assert "## When not to use" not in out
        assert "## See also" not in out

    def test_reference_links_are_percent_encoded(self) -> None:
        # Schema ids can never contain spaces, but the renderer encodes
        # defensively (research §11 belt-and-braces).
        ds = DatasetEntry(schema_id="a b", schema_ref="a b@1.0.0", title=None, row_count=0)
        out = SkillRenderer().render_skill(node=_node(), base_url=BASE, datasets=[ds], docs=[])
        assert "api/v1/data/a%20b.md" in out
