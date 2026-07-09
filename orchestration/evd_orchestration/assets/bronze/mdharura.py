from .ingest import build_bronze_asset

bronze_mdharura_raw = build_bronze_asset("mdharura", folder="signals")
