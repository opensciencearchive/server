"""Open Scientific Archive."""

import warnings

# `schema` is the natural wire name for a schema reference across the /data/
# and MCP surfaces, and it shadows only the DEPRECATED `BaseModel.schema()`
# classmethod (removed access path in Pydantic v2 — we never call it).
# Silence exactly that shadow warning; any other shadowed field still warns.
warnings.filterwarnings(
    "ignore",
    message=r'Field name "schema" in ".*" shadows an attribute in parent',
    category=UserWarning,
)
