from .ingest import build_bronze_asset

bronze_taifa_care_kenyaemr_raw = build_bronze_asset("taifa_care_kenyaemr", folder="flagged_cases")
