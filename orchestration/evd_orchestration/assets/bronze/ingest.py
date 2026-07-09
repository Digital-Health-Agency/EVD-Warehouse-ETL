import uuid
from datetime import datetime, timezone
from typing import Any

from dagster import AssetExecutionContext, AssetsDefinition, MetadataValue, asset
from psycopg2 import sql
from psycopg2.extras import execute_values

from evd_orchestration.resources import DuckDBResource, MinIOResource, PostgresResource

from .schema_inference import (
    canonical_hash,
    diff_new_columns,
    flatten_record,
    infer_pg_type,
    sanitize_column_name,
)

BRONZE_SCHEMA = "bronze"
ENVELOPE_COLUMNS = [
    "_ingested_at",
    "_source",
    "_batch_id",
    "_source_file",
    "_raw_hash",
    "_processed",
]
# "id" is the surrogate BIGSERIAL PRIMARY KEY (see _ensure_table); the rest are
# envelope columns with their reserved leading underscore stripped. A sanitized
# data column landing on any of these would silently collide with a reserved
# column instead of getting its own — see sanitize_column_name.
RESERVED_COLUMN_NAMES = frozenset({"id"} | {c.lstrip("_") for c in ENVELOPE_COLUMNS})


def read_source_records(duckdb: DuckDBResource, bucket: str, key: str) -> list[dict]:
    """Read one MinIO object as a list of nested Python dicts via DuckDB.

    `format='auto'` handles a single JSON object, a JSON array, or NDJSON
    uniformly; `compression='auto'` sniffs gzip regardless of file extension.
    """
    conn = duckdb.get_connection()
    try:
        cursor = conn.execute(
            f"SELECT * FROM read_json_auto('s3://{bucket}/{key}', format='auto', "
            "compression='auto_detect')"
        )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    finally:
        conn.close()
    return [dict(zip(columns, row)) for row in rows]


def _ensure_table(cursor, table: str) -> None:
    cursor.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
                id BIGSERIAL PRIMARY KEY,
                _ingested_at TIMESTAMPTZ NOT NULL,
                _source TEXT NOT NULL,
                _batch_id UUID NOT NULL,
                _source_file TEXT NOT NULL,
                _raw_hash TEXT NOT NULL UNIQUE,
                _processed BOOLEAN NOT NULL DEFAULT FALSE
            )
            """
        ).format(sql.Identifier(BRONZE_SCHEMA), sql.Identifier(table))
    )


def _existing_columns(cursor, table: str) -> dict[str, str]:
    cursor.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (BRONZE_SCHEMA, table),
    )
    return {row[0]: row[1] for row in cursor.fetchall()}


def _evolve_schema(cursor, table: str, required_columns: dict[str, str]) -> list[str]:
    _ensure_table(cursor, table)
    existing = _existing_columns(cursor, table)
    new_columns = diff_new_columns(existing, required_columns)
    for column, pg_type in new_columns.items():
        cursor.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {}").format(
                sql.Identifier(BRONZE_SCHEMA),
                sql.Identifier(table),
                sql.Identifier(column),
                sql.SQL(pg_type),
            )
        )
    return list(new_columns)


def _coerce(value: Any, pg_type: str) -> Any:
    """Match a value to the column's already-established type.

    A field's type is fixed by whichever value was seen first for it; if a
    later record disagrees (e.g. a boolean flag later sent as text, or vice
    versa), coerce rather than let the insert fail — bronze must never reject
    a batch over a type surprise.
    """
    if value is None:
        return None
    if pg_type == "boolean":
        return value if isinstance(value, bool) else str(value)
    return str(value) if isinstance(value, bool) else value


def _insert_rows(cursor, table: str, columns: list[str], rows: list[tuple]) -> int:
    if not rows:
        return 0
    insert_stmt = sql.SQL(
        "INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT (_raw_hash) DO NOTHING"
    ).format(
        sql.Identifier(BRONZE_SCHEMA),
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
    )
    execute_values(cursor, insert_stmt, rows)
    return cursor.rowcount


def build_bronze_asset(source: str, folder: str | None = "records") -> AssetsDefinition:
    """Factory: one Dagster asset per sending system.

    `f"s3://{bucket}/{source}_raw/{folder}/"` -> `bronze.{source}_raw`, moving
    each file to `{source}_raw/{folder}/` -> `_processed_{source}_raw/{folder}/`
    only after it's durably committed to Postgres. A future sending system is
    `build_bronze_asset("<system>", folder="<entity>")` — no other code changes.

    `folder=None` defers picking the sub-folder to run time: used when the
    sending system's folder name under `{source}_raw/` isn't known yet (or
    may not exist yet). Each run looks for exactly one sub-folder that isn't
    a `_dlt*` bookkeeping folder and uses that; if none exist yet, or more
    than one candidate is found, the run skips rather than guessing.
    """
    table = f"{source}_raw"

    @asset(name=f"bronze_{table}")
    def _bronze_asset(
        context: AssetExecutionContext,
        duckdb: DuckDBResource,
        minio: MinIOResource,
        postgres: PostgresResource,
    ) -> None:
        if folder is not None:
            records_prefix = f"{source}_raw/{folder}/"
        else:
            base = f"{source}_raw/"
            candidates = [
                p for p in minio.list_prefixes(base) if not p[len(base):].startswith("_dlt")
            ]
            if not candidates:
                context.log.info(f"No data folder yet under {base} — skipping")
                return
            if len(candidates) > 1:
                context.log.warning(
                    f"Ambiguous data folders under {base}: {candidates} — skipping until resolved"
                )
                return
            records_prefix = candidates[0]

        processed_prefix = "_processed_" + records_prefix

        keys = minio.list_keys(records_prefix)
        if not keys:
            context.log.info(f"No new files under {records_prefix} — skipping")
            return

        files_processed = 0
        rows_inserted = 0
        rows_deduped = 0
        columns_added: set[str] = set()

        for key in keys:
            raw_records = read_source_records(duckdb, minio.bucket, key)
            if not raw_records:
                context.log.warning(f"{key}: no records parsed — skipping, not moving")
                continue

            flattened: list[tuple[dict, dict]] = []
            required_columns: dict[str, str] = {}
            for record in raw_records:
                flat = {
                    sanitize_column_name(k, reserved=RESERVED_COLUMN_NAMES): v
                    for k, v in flatten_record(record).items()
                }
                for col, value in flat.items():
                    required_columns.setdefault(col, infer_pg_type(value))
                flattened.append((record, flat))

            batch_id = str(uuid.uuid4())
            ingested_at = datetime.now(timezone.utc)

            with postgres.get_connection() as pg:
                with pg.cursor() as cursor:
                    added = _evolve_schema(cursor, table, required_columns)
                    columns_added.update(added)
                    column_types = _existing_columns(cursor, table)

                    data_columns = sorted(required_columns)
                    all_columns = ENVELOPE_COLUMNS + data_columns
                    rows = []
                    for record, flat in flattened:
                        row = [ingested_at, source, batch_id, key, canonical_hash(record), False]
                        row.extend(
                            _coerce(flat.get(col), column_types[col]) for col in data_columns
                        )
                        rows.append(tuple(row))

                    inserted = _insert_rows(cursor, table, all_columns, rows)
                    rows_inserted += inserted
                    rows_deduped += len(rows) - inserted

            minio.move_to_processed(key, records_prefix, processed_prefix)
            files_processed += 1
            context.log.info(f"{key}: staged {len(flattened)} rows, moved to processed")

        if files_processed:
            minio.ensure_prefix_marker(records_prefix)

        context.add_output_metadata(
            {
                "files_processed": files_processed,
                "rows_inserted": rows_inserted,
                "rows_deduped": rows_deduped,
                "columns_added": MetadataValue.json(sorted(columns_added)),
            }
        )

    return _bronze_asset
