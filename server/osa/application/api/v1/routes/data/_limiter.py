"""Shared slowapi limiter for ``/data/`` POST routes (research §5).

POST routes accept structured ``FilterExpr`` bodies that can express expensive
queries, so they are rate-limited per source IP. GET routes are intentionally
unlimited — stable GET URLs are exactly what we want CDNs to cache and
consumers to curl on a cron.

Default: 10 requests/minute/IP on POST routes. The limiter instance is shared
(module singleton) and registered on the FastAPI app at startup; route handlers
apply it via ``@limiter.limit(POST_RATE_LIMIT)``.
"""

from __future__ import annotations

from slowapi import Limiter
from slowapi.util import get_remote_address

POST_RATE_LIMIT = "10/minute"

limiter = Limiter(key_func=get_remote_address)
