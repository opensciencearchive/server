"""API-to-storage naming translation.

The API surface and the PG storage layout coincidentally share names today —
feature references in the discovery wire format say ``features.<hook>.<column>``
and that maps cleanly onto ``features."<hook>".<column>`` in PostgreSQL. The
metadata tables in the ``metadata`` PG schema likewise mirror the API's
``metadata.<field>`` prefix.

This module is the seam between the API and the storage layer. Callers route
through these functions so that if the API naming ever needs to diverge from
the PG layout (API rename; storage consolidation; federation-driven rename),
the translation lives here rather than being sprinkled through adapters and
stores.

All functions are identity implementations today. The point is to *mark the
boundary* so it is crossable later, not to make the names different now.
"""

from __future__ import annotations


def feature_pg_schema() -> str:
    """PG schema name holding dynamic feature tables.

    Mirrors the API's ``features.*`` prefix today.
    """
    return "features"


def feature_pg_table(api_feature_name: str) -> str:
    """PG table name for a feature referenced by its API name.

    The ``<hook>`` segment of the API path ``features.<hook>.<column>`` maps
    to this PG table name. Identity today — the API and PG names are
    intentionally aligned for readability. Introduce a real mapping here if
    the two ever diverge.
    """
    return api_feature_name


def metadata_pg_schema() -> str:
    """PG schema name holding dynamic per-schema metadata tables.

    Mirrors the API's ``metadata.*`` prefix today.
    """
    return "metadata"
