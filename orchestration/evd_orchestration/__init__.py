import os

from dagster import Definitions
from dagster_dbt import DbtCliResource

from evd_orchestration.assets import (
    bronze_adam_cases_raw,
    bronze_adam_travellers_raw,
    bronze_cbs_raw,
    bronze_echis_raw,
    bronze_krcs_evd_screening_raw,
    bronze_lims_raw,
    bronze_mdharura_raw,
    evd_dbt_assets,
)
from evd_orchestration.assets.transform import DBT_PROFILES_DIR, DBT_PROJECT_DIR
from evd_orchestration.jobs import dbt_job, ingest_job
from evd_orchestration.resources import DuckDBResource, MinIOResource, PostgresResource
from evd_orchestration.schedules import lims_daily_schedule

defs = Definitions(
    assets=[
        bronze_lims_raw,
        bronze_adam_cases_raw,
        bronze_adam_travellers_raw,
        bronze_cbs_raw,
        bronze_mdharura_raw,
        bronze_krcs_evd_screening_raw,
        bronze_echis_raw,
        evd_dbt_assets,
    ],
    jobs=[ingest_job, dbt_job],
    schedules=[lims_daily_schedule],
    resources={
        "minio": MinIOResource(
            endpoint_url=os.environ["MINIO_ENDPOINT"],
            access_key=os.environ["MINIO_ROOT_USER"],
            secret_key=os.environ["MINIO_ROOT_PASSWORD"],
            bucket=os.environ.get("MINIO_BUCKET", "evd"),
        ),
        "postgres": PostgresResource(
            host=os.environ["PG_HOST"],
            port=int(os.environ["PG_PORT"]),
            dbname=os.environ["PG_DBNAME"],
            user=os.environ["PG_USER"],
            password=os.environ["PG_PASSWORD"],
        ),
        "duckdb": DuckDBResource(
            s3_endpoint=os.environ["MINIO_ENDPOINT"],
            s3_access_key=os.environ["MINIO_ROOT_USER"],
            s3_secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        ),
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROFILES_DIR),
        ),
    },
)
