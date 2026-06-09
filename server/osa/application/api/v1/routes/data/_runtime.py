"""Per-route Postgres statement-timeout dependency (research §7).

Each :class:`DataResponseFormat` carries its own ``statement_timeout`` (30s for
paginated JSON, 30min for streaming dumps). ``apply_statement_timeout`` issues
``SET LOCAL statement_timeout = '...'`` against the active session at the start
of the request; ``SET LOCAL`` reverts on commit/rollback so the value never
leaks to a later request reusing the pooled connection.

``idle_in_transaction_session_timeout`` is set globally in Postgres config (see
quickstart), not per-route — it's the same everywhere and protects the pool
from any route's misbehaviour.
"""

from __future__ import annotations

import re

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.data.model.format import DataResponseFormat

# Timeout literals are operator-controlled (from FORMATS), never user input, but
# validate anyway so a malformed registry value can't reach raw SQL.
_TIMEOUT_RE = re.compile(r"^\d+(ms|s|min)?$")


async def apply_statement_timeout(session: AsyncSession, fmt: DataResponseFormat) -> None:
    timeout = fmt.statement_timeout
    if not _TIMEOUT_RE.match(timeout):
        raise ValueError(f"Invalid statement_timeout literal: {timeout!r}")
    await session.execute(text(f"SET LOCAL statement_timeout = '{timeout}'"))
