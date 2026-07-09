from .ingest import build_bronze_asset

bronze_cbs_raw = build_bronze_asset("cbs", folder="reports")
