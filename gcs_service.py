"""
GCS service: read-only access to GCS buckets.

Covers two domains:
  - SQL transformation scripts  (SQL_GCS_BUCKET / SQL_GCS_PREFIX)
  - Composer DAG files          (bucket discovered via composer_service or COMPOSER_DAG_BUCKET)
"""
import logging
import re
from typing import Any

import settings

log = logging.getLogger(__name__)
_client = None


def _get_client():
    global _client
    if _client is None:
        from google.cloud import storage
        project = settings.COMPOSER_PROJECT_ID or settings.GCP_PROJECT_ID or None
        _client = storage.Client(project=project)
    return _client


def _normalize_name(name: str) -> str:
    """Reduce any naming convention to lowercase_snake_case for fuzzy matching."""
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


# ---------------------------------------------------------------------------
# Low-level helpers (used internally and by composer_service)
# ---------------------------------------------------------------------------

def list_blobs(bucket_name: str, prefix: str = "", suffix: str = "") -> dict[str, Any]:
    """List blobs in *bucket_name* optionally filtered by prefix and suffix."""
    if not bucket_name:
        return {"error": "bucket_name is required"}
    try:
        blobs = _get_client().list_blobs(bucket_name, prefix=prefix or None)
        paths = [b.name for b in blobs if not suffix or b.name.endswith(suffix)]
        return {"bucket": bucket_name, "prefix": prefix, "files": paths, "count": len(paths)}
    except Exception as exc:
        log.warning("list_blobs(%s, %s): %s", bucket_name, prefix, exc)
        return {"error": str(exc)}


def read_blob(bucket_name: str, path: str) -> dict[str, Any]:
    """Download and return the text content of a single GCS blob."""
    if not bucket_name:
        return {"error": "bucket_name is required"}
    try:
        blob = _get_client().bucket(bucket_name).blob(path)
        content = blob.download_as_text()
        return {
            "bucket": bucket_name,
            "path": path,
            "gcs_uri": f"gs://{bucket_name}/{path}",
            "content": content,
        }
    except Exception as exc:
        log.warning("read_blob(%s, %s): %s", bucket_name, path, exc)
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# SQL script tools
# ---------------------------------------------------------------------------

def list_sql_files(prefix: str = "") -> dict[str, Any]:
    """List all .sql files in the configured SQL GCS bucket."""
    if not settings.SQL_GCS_BUCKET:
        return {"error": "SQL_GCS_BUCKET not configured"}
    effective_prefix = prefix or settings.SQL_GCS_PREFIX
    return list_blobs(settings.SQL_GCS_BUCKET, prefix=effective_prefix, suffix=".sql")


def find_sql_for_table(table_name: str) -> dict[str, Any]:
    """
    Search the SQL GCS bucket for a .sql file whose stem matches *table_name*.

    Matching priority:
      1. Exact stem match (case-insensitive normalized)
      2. Stem contains or is contained by normalized table name
      3. Any shared token

    Returns the best match with its full content.
    """
    if not settings.SQL_GCS_BUCKET:
        return {"error": "SQL_GCS_BUCKET not configured"}

    listing = list_sql_files()
    if "error" in listing:
        return listing

    sql_files: list[str] = listing.get("files", [])
    if not sql_files:
        return {"error": "No .sql files found in the configured bucket/prefix"}

    target = _normalize_name(table_name)

    def _score(path: str) -> int:
        stem = _normalize_name(re.sub(r"\.sql$", "", path.split("/")[-1], flags=re.IGNORECASE))
        if stem == target:
            return 4
        if stem.startswith(target) or target.startswith(stem):
            return 3
        if target in stem or stem in target:
            return 2
        if set(target.split("_")) & set(stem.split("_")):
            return 1
        return 0

    ranked = sorted(sql_files, key=_score, reverse=True)
    best_score = _score(ranked[0]) if ranked else 0

    if best_score == 0:
        return {
            "error": f"No SQL file found matching '{table_name}'",
            "hint": "Call list_sql_files() to browse available files",
            "available_count": len(sql_files),
        }

    best = ranked[0]
    result = read_blob(settings.SQL_GCS_BUCKET, best)
    result["table_name"] = table_name
    result["match_score"] = best_score
    result["other_candidates"] = [p for p in ranked[1:5] if _score(p) > 0]
    return result


def read_sql_file(path: str) -> dict[str, Any]:
    """Read a specific .sql file from the SQL GCS bucket by its blob path."""
    if not settings.SQL_GCS_BUCKET:
        return {"error": "SQL_GCS_BUCKET not configured"}
    return read_blob(settings.SQL_GCS_BUCKET, path)


def read_gcs_path(gcs_uri: str) -> dict[str, Any]:
    """
    Read any GCS file by its full gs:// URI.

    Args:
        gcs_uri: full GCS URI in the form gs://bucket-name/path/to/file.sql

    Returns the file content together with bucket, path, and gcs_uri for citation.
    Access is governed by the application's GCP credentials (IAM).
    """
    if not gcs_uri.startswith("gs://"):
        return {"error": "gcs_uri must start with gs:// (e.g. gs://my-bucket/sql/orders.sql)"}
    remainder = gcs_uri[5:]  # strip "gs://"
    if "/" not in remainder:
        return {"error": "Invalid gs:// URI: no object path found after bucket name"}
    bucket, path = remainder.split("/", 1)
    if not bucket or not path:
        return {"error": f"Could not parse bucket and path from '{gcs_uri}'"}
    return read_blob(bucket, path)
