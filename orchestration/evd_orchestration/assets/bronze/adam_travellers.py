from .ingest import build_bronze_asset

bronze_adam_travellers_raw = build_bronze_asset("adam_travellers", folder="travellers")
