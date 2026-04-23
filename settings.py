import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

GCP_PROJECT_ID     = os.getenv("GCP_PROJECT_ID", "")
VERTEX_LOCATION    = os.getenv("VERTEX_LOCATION", "us-central1")

BQ_PROJECTS        = [p.strip() for p in os.getenv("BQ_PROJECTS", GCP_PROJECT_ID).split(",") if p.strip()]
BQ_BILLING_PROJECT = os.getenv("BQ_BILLING_PROJECT", BQ_PROJECTS[0] if BQ_PROJECTS else "")
MAX_SQL_ROWS       = int(os.getenv("MAX_SQL_ROWS", "0"))

# Excel – general data loading into DuckDB
EXCEL_DATA_PATH    = os.getenv("EXCEL_DATA_PATH", "")
EXCEL_HEADER_ROWS  = int(os.getenv("EXCEL_HEADER_ROWS", "1"))

# Excel – spec classification (folder-based)
# EXCEL_MAPPING_PATH : folder (and all subfolders) whose files are treated as Mapping Sheets
#                      Every other Excel file under EXCEL_DATA_PATH is treated as a Master Sheet.
# EXCEL_MASTER_HEADER_ROWS : number of header rows in Master Sheet files (default 1)
# EXCEL_MAPPING_HEADER_ROWS: number of header rows in Mapping Sheet files (default 2)
EXCEL_MASTER_PATH         = os.getenv("EXCEL_MASTER_PATH", "")
EXCEL_MAPPING_PATH        = os.getenv("EXCEL_MAPPING_PATH", "")
EXCEL_MASTER_HEADER_ROWS  = int(os.getenv("EXCEL_MASTER_HEADER_ROWS", "1"))
EXCEL_MAPPING_HEADER_ROWS = int(os.getenv("EXCEL_MAPPING_HEADER_ROWS", "2"))

# GCS – SQL transformation scripts
SQL_GCS_BUCKET = os.getenv("SQL_GCS_BUCKET", "")
SQL_GCS_PREFIX = os.getenv("SQL_GCS_PREFIX", "")  # e.g. "sql/transformations/"

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

LOG_LEVEL          = os.getenv("LOG_LEVEL", "INFO")

# ── Schema Audit (MySQL → BigQuery reconciliation) ────────────────────────────
# GCP project that hosts the HEADER_VIEW and DETAIL_VIEW metadata tables.
SCHEMA_METADATA_PROJECT = os.getenv("SCHEMA_METADATA_PROJECT", GCP_PROJECT_ID)

# BQ target projects — prod for deployed_to_prod=1, UAT for everything else.
SCHEMA_BQ_PROJECT_PROD  = os.getenv("SCHEMA_BQ_PROJECT_PROD", "")
SCHEMA_BQ_PROJECT_UAT   = os.getenv("SCHEMA_BQ_PROJECT_UAT",  SCHEMA_METADATA_PROJECT)

# Fully-qualified BigQuery views for streamed-table metadata.
# Defaults assume dataset raw_sls_v in SCHEMA_METADATA_PROJECT.
SCHEMA_HEADER_VIEW = os.getenv(
    "SCHEMA_HEADER_VIEW",
    f"{SCHEMA_METADATA_PROJECT}.raw_sls_v.datstream_header_v" if SCHEMA_METADATA_PROJECT else "",
)
SCHEMA_DETAIL_VIEW = os.getenv(
    "SCHEMA_DETAIL_VIEW",
    f"{SCHEMA_METADATA_PROJECT}.raw_sls_v.datstream_detail_v" if SCHEMA_METADATA_PROJECT else "",
)

# Output directory for generated Excel reports and DDL JSON files.
SCHEMA_AUDIT_OUTPUT_DIR = os.getenv("SCHEMA_AUDIT_OUTPUT_DIR", ".")
