# Sensors watch for external events (e.g. new files landing under a
# `*_raw/records/` prefix in MinIO) and trigger jobs. Add MinIO event sensors
# here as sources are added; the schedule above is the interim polling path.

from dagster import DagsterRunStatus, RunRequest, run_status_sensor

from evd_orchestration.jobs import dbt_job, ingest_job


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[ingest_job],
    request_job=dbt_job,
)
def dbt_after_ingest_sensor():
    return RunRequest()
