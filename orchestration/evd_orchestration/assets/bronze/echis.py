from .ingest import build_bronze_asset

bronze_echis_raw = build_bronze_asset("echis", folder="signals")
