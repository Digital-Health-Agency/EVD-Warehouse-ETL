from .ingest import build_bronze_asset

# nphiis_raw/ has multiple sibling data folders; each is staged as its own table.
bronze_nphiis_cases_raw = build_bronze_asset("nphiis", folder="cases", table="nphiis_cases_raw")
bronze_nphiis_contacts_raw = build_bronze_asset(
    "nphiis", folder="contacts", table="nphiis_contacts_raw"
)
