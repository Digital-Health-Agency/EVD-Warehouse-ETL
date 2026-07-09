from .ingest import build_bronze_asset

# Folder name under `krcs_evd_screening_raw/` isn't known yet — `folder=None`
# discovers it at run time and skips if it isn't there yet.
bronze_krcs_evd_screening_raw = build_bronze_asset("krcs_evd_screening", folder=None)
