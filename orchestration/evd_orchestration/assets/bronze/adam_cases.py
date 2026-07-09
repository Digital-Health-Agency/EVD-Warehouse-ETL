from .ingest import build_bronze_asset

bronze_adam_cases_raw = build_bronze_asset("adam_cases", folder="cases")
