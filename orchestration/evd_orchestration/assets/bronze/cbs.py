from .ingest import build_bronze_asset

# cbs_raw/ has multiple sibling data folders; each is staged as its own table.
bronze_cbs_raw = build_bronze_asset("cbs", folder="reports")
bronze_cbs_screenings_raw = build_bronze_asset(
    "cbs", folder="screenings", table="cbs_screenings_raw"
)
