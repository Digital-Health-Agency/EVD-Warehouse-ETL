from .ingest import build_bronze_asset

bronze_krcs_evd_quarantine_raw = build_bronze_asset(
    "krcs_evd_quarantine", folder="quarantine_records"
)
