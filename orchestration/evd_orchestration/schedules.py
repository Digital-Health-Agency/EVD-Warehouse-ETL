from dagster import ScheduleDefinition

from evd_orchestration.jobs import ingest_job

ingest_daily_schedule = ScheduleDefinition(
    job=ingest_job,
    cron_schedule="0 1 * * *",
)
