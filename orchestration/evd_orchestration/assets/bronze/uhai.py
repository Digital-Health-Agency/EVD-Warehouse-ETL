from .ingest import build_bronze_asset

bronze_uhai_raw = build_bronze_asset("uhai", folder="traveler_screenings")
