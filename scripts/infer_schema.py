#!/usr/bin/env python
"""Dry-run schema exploration: sample MinIO files, print the inferred bronze
schema (CREATE TABLE / ALTER TABLE ADD COLUMN preview). No MinIO move, no DB
writes — safe to run before Postgres credentials exist.

Usage: uv run python scripts/infer_schema.py --source lims --sample 20
"""

import argparse
import os

from evd_orchestration.assets.bronze.ingest import read_source_records
from evd_orchestration.assets.bronze.schema_inference import (
    flatten_record,
    infer_pg_type,
    sanitize_column_name,
)
from evd_orchestration.resources import DuckDBResource, MinIOResource


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, help="Sending system, e.g. lims")
    parser.add_argument(
        "--folder",
        default="records",
        help="Data sub-folder under {source}_raw/, e.g. records, cases, reports",
    )
    parser.add_argument("--sample", type=int, default=20, help="Max number of files to sample")
    args = parser.parse_args()

    minio = MinIOResource(
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        bucket=os.environ.get("MINIO_BUCKET", "evd"),
    )
    duckdb = DuckDBResource(
        s3_endpoint=os.environ["MINIO_ENDPOINT"],
        s3_access_key=os.environ["MINIO_ROOT_USER"],
        s3_secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    )

    prefix = f"{args.source}_raw/{args.folder}/"
    keys = minio.list_keys(prefix)
    if not keys:
        print(f"No files found under s3://{minio.bucket}/{prefix}")
        return

    sample_keys = keys[: args.sample]
    print(f"Sampling {len(sample_keys)} of {len(keys)} files under {prefix}\n")

    required_columns: dict[str, str] = {}
    total_records = 0
    for key in sample_keys:
        records = read_source_records(duckdb, minio.bucket, key)
        total_records += len(records)
        for record in records:
            flat = {sanitize_column_name(k): v for k, v in flatten_record(record).items()}
            for col, value in flat.items():
                required_columns.setdefault(col, infer_pg_type(value))

    print(f"{total_records} records parsed, {len(required_columns)} columns inferred\n")

    table = f"{args.source}_raw"
    print(f"-- bronze.{table} — envelope columns are fixed, data columns evolve additively")
    print(f'CREATE TABLE IF NOT EXISTS bronze."{table}" (')
    print("    id BIGSERIAL PRIMARY KEY,")
    print("    _ingested_at TIMESTAMPTZ NOT NULL,")
    print("    _source TEXT NOT NULL,")
    print("    _batch_id UUID NOT NULL,")
    print("    _source_file TEXT NOT NULL,")
    print("    _raw_hash TEXT NOT NULL UNIQUE,")
    print("    _processed BOOLEAN NOT NULL DEFAULT FALSE")
    print(");\n")

    print("-- columns inferred from this sample:")
    for col in sorted(required_columns):
        pg_type = required_columns[col]
        print(f'ALTER TABLE bronze."{table}" ADD COLUMN IF NOT EXISTS "{col}" {pg_type};')


if __name__ == "__main__":
    main()
