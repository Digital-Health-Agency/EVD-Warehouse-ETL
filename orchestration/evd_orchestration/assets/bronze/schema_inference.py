"""Pure, I/O-free functions for inferring and evolving a Postgres bronze schema
from nested JSON records. Flattening is fully recursive (dot-notation columns,
arrays index-flattened); typing is deliberately simple — BOOLEAN for real
booleans, TEXT for everything else — so bronze evolution is always additive
(new columns only, never ALTER COLUMN TYPE).
"""

import hashlib
import json
import re
from typing import Any

_IDENTIFIER_MAX_LEN = 63
_INVALID_CHARS = re.compile(r"[^a-z0-9_]")


def flatten_record(record: dict, sep: str = "__", max_depth: int = 6) -> dict[str, Any]:
    """Recursively flatten nested dicts/lists into scalar leaves.

    Dict keys join with `sep`; list items join with their index. Anything still
    a dict/list past `max_depth` collapses to a JSON string rather than
    continuing to explode columns.
    """
    flat: dict[str, Any] = {}

    def _walk(value: Any, path: str, depth: int) -> None:
        if isinstance(value, dict) and depth < max_depth:
            if not value:
                flat[path] = None
                return
            for key, sub_value in value.items():
                _walk(sub_value, f"{path}{sep}{key}" if path else str(key), depth + 1)
        elif isinstance(value, list) and depth < max_depth:
            if not value:
                flat[path] = None
                return
            for index, item in enumerate(value):
                _walk(item, f"{path}{sep}{index}" if path else str(index), depth + 1)
        elif isinstance(value, (dict, list)):
            flat[path] = json.dumps(value, sort_keys=True, default=str)
        elif value is None or isinstance(value, bool):
            flat[path] = value
        else:
            # DuckDB's JSON reader sniffs shape and hands back rich types for
            # plain JSON strings (uuid.UUID, datetime.date/datetime, Decimal,
            # ...) instead of str. Bronze's typing is intentionally two-valued
            # (BOOLEAN/TEXT) — normalize every non-bool leaf to str here so
            # infer_pg_type/_coerce never see anything psycopg2 can't adapt.
            flat[path] = str(value)

    for key, value in record.items():
        _walk(value, str(key), 0)

    return flat


def sanitize_column_name(path: str, reserved: frozenset[str] = frozenset()) -> str:
    """Turn a flattened field path into a valid, unique Postgres identifier.

    Leading/trailing underscores are always stripped — this is what keeps
    sanitized data columns from ever colliding with the `_`-prefixed envelope
    columns (`_ingested_at`, `_raw_hash`, ...), which are reserved. But
    stripping the leading underscore can itself produce a collision: a source
    field literally named `_id` (e.g. a Mongo-shaped export) sanitizes to
    `id`, which is also the surrogate `BIGSERIAL PRIMARY KEY` column — without
    disambiguation the source's string id would silently target that bigint
    column instead of getting its own. `reserved` (envelope names stripped of
    their leading underscore, plus `id`) catches that case.
    """
    # Note: no collapsing of repeated underscores here — `__` is flatten_record's
    # nesting separator and must survive sanitization intact.
    name = _INVALID_CHARS.sub("_", path.lower()).strip("_")
    if not name:
        name = "field"
    if name[0].isdigit():
        name = f"_{name}"
    if name in reserved:
        name = f"{name}_field"
    if len(name) > _IDENTIFIER_MAX_LEN:
        digest = hashlib.md5(name.encode()).hexdigest()[:8]
        name = f"{name[:_IDENTIFIER_MAX_LEN - 9]}_{digest}"
    return name


def infer_pg_type(value: Any) -> str:
    """BOOLEAN for real booleans, TEXT for everything else (including None)."""
    return "boolean" if isinstance(value, bool) else "text"


def canonical_hash(record: dict) -> str:
    """Stable hash of the original nested record, used for `_raw_hash` dedup.

    Computed on the nested record (not the flattened one) so it stays stable
    even if flattening rules change later.
    """
    canonical = json.dumps(record, sort_keys=True, default=str)
    return hashlib.md5(canonical.encode()).hexdigest()


def diff_new_columns(existing: dict[str, str], required: dict[str, str]) -> dict[str, str]:
    """Columns present in `required` but not yet in `existing` — additive only."""
    return {col: pg_type for col, pg_type in required.items() if col not in existing}
