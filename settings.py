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

# Composer V3 – targeted environment connection
COMPOSER_PROJECT_ID  = os.getenv("COMPOSER_PROJECT_ID", GCP_PROJECT_ID)
COMPOSER_LOCATION    = os.getenv("COMPOSER_LOCATION", VERTEX_LOCATION)
COMPOSER_ENVIRONMENT = os.getenv("COMPOSER_ENVIRONMENT", "")
COMPOSER_DAG_BUCKET  = os.getenv("COMPOSER_DAG_BUCKET", "")  # leave blank to auto-discover via API
AIRFLOW_API_TIMEOUT  = int(os.getenv("AIRFLOW_API_TIMEOUT", "30"))  # seconds

# GCS – SQL transformation scripts
SQL_GCS_BUCKET = os.getenv("SQL_GCS_BUCKET", "")
SQL_GCS_PREFIX = os.getenv("SQL_GCS_PREFIX", "")  # e.g. "sql/transformations/"

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

LOG_LEVEL          = os.getenv("LOG_LEVEL", "INFO")

# ── Git Repository ────────────────────────────────────────────────────────────
# Remote URL (HTTPS or SSH).  Leave blank to use a pre-existing local clone only.
GIT_REPO_URL   = os.getenv("GIT_REPO_URL", "")

# Branch to checkout and push to (leave blank to use the repo default branch).
GIT_BRANCH     = os.getenv("GIT_BRANCH", "main")

# Absolute path where the repo is (or will be) cloned on this machine.
GIT_LOCAL_PATH = os.getenv("GIT_LOCAL_PATH", "/tmp/dbconnect_git_repo")

# Personal Access Token for HTTPS auth.  Omit when using SSH key auth.
GIT_TOKEN      = os.getenv("GIT_TOKEN", "")

# Subdirectory inside the repo that holds Airflow DAG (.py) files.
GIT_DAG_PATH   = os.getenv("GIT_DAG_PATH", "dags/")

# Subdirectory inside the repo that holds SQL transformation (.sql) files.
GIT_SQL_PATH   = os.getenv("GIT_SQL_PATH", "sql/")

# Committer identity used when writing back optimised files.
GIT_COMMIT_USER_NAME  = os.getenv("GIT_COMMIT_USER_NAME", "MyAgent")
GIT_COMMIT_USER_EMAIL = os.getenv("GIT_COMMIT_USER_EMAIL", "dbconnect@example.com")
