"""Authorization actions — all operations subject to access control."""

from enum import StrEnum


class Action(StrEnum):
    """Structured enum of all authorization-relevant operations."""

    # Depositions
    DEPOSITION_CREATE = "deposition:create"
    DEPOSITION_READ = "deposition:read"
    DEPOSITION_UPDATE = "deposition:update"
    DEPOSITION_SUBMIT = "deposition:submit"
    DEPOSITION_DELETE = "deposition:delete"

    # Curation
    DEPOSITION_APPROVE = "deposition:approve"
    DEPOSITION_REJECT = "deposition:reject"

    # Registry — Schemas
    SCHEMA_READ = "schema:read"
    SCHEMA_CREATE = "schema:create"
    SCHEMA_UPDATE = "schema:update"
    SCHEMA_DELETE = "schema:delete"

    # Registry — Traits
    TRAIT_READ = "trait:read"
    TRAIT_CREATE = "trait:create"
    TRAIT_UPDATE = "trait:update"
    TRAIT_DELETE = "trait:delete"

    # Registry — Conventions
    CONVENTION_READ = "convention:read"
    CONVENTION_CREATE = "convention:create"
    CONVENTION_UPDATE = "convention:update"
    CONVENTION_DELETE = "convention:delete"

    # Registry — Vocabularies
    VOCABULARY_READ = "vocabulary:read"
    VOCABULARY_CREATE = "vocabulary:create"
    VOCABULARY_UPDATE = "vocabulary:update"
    VOCABULARY_DELETE = "vocabulary:delete"

    # Records (read-only after publication)
    RECORD_READ = "record:read"

    # Search
    SEARCH_QUERY = "search:query"

    # Validation
    VALIDATION_CREATE = "validation:create"
    VALIDATION_READ = "validation:read"

    # Administration
    ROLE_ASSIGN = "role:assign"
    ROLE_REVOKE = "role:revoke"
    ROLE_READ = "role:read"
