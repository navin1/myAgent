"""
Composer V3 service: connect to a specific Airflow environment, discover DAG files
from its GCS bucket, and locate which DAG implements a given table.
"""
import logging
import re
from typing import Any

import settings

log = logging.getLogger(__name__)

# Cached (bucket, dags_prefix) for the configured environment
_dag_location: tuple[str, str] | None = None


def _requires_composer() -> str | None:
    """Return an error string if required Composer settings are missing, else None."""
    missing = [
        k for k, v in {
            "COMPOSER_PROJECT_ID": settings.COMPOSER_PROJECT_ID,
            "COMPOSER_LOCATION": settings.COMPOSER_LOCATION,
            "COMPOSER_ENVIRONMENT": settings.COMPOSER_ENVIRONMENT,
        }.items() if not v
    ]
    return f"Missing required settings: {', '.join(missing)}" if missing else None


def _environment_resource_name() -> str:
    return (
        f"projects/{settings.COMPOSER_PROJECT_ID}"
        f"/locations/{settings.COMPOSER_LOCATION}"
        f"/environments/{settings.COMPOSER_ENVIRONMENT}"
    )


def _composer_rest_session():
    """Return a requests.Session with a fresh GCP Bearer token (cloud-platform scope)."""
    import requests
    import google.auth
    import google.auth.transport.requests as google_requests

    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(google_requests.Request())
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {credentials.token}",
        "Content-Type": "application/json",
    })
    return session


def list_composer_environments(location: str = "") -> dict[str, Any]:
    """
    List all Composer environments in the configured project via the Composer REST API.

    Args:
        location: GCP region (e.g. "us-central1"). Defaults to COMPOSER_LOCATION.
                  Pass "-" to list across all regions.
    """
    if not settings.COMPOSER_PROJECT_ID:
        return {"error": "COMPOSER_PROJECT_ID is not configured"}

    loc = location or settings.COMPOSER_LOCATION or "-"
    url = (
        f"https://composer.googleapis.com/v1/projects"
        f"/{settings.COMPOSER_PROJECT_ID}/locations/{loc}/environments"
    )
    try:
        import requests as _requests
        session = _composer_rest_session()
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for env in data.get("environments", []):
            name_parts = env.get("name", "").split("/")
            cfg = env.get("config", {})
            results.append({
                "name": name_parts[-1] if name_parts else env.get("name", ""),
                "location": name_parts[-3] if len(name_parts) >= 3 else loc,
                "state": env.get("state", ""),
                "airflow_uri": cfg.get("airflowUri", ""),
                "gcs_dag_prefix": cfg.get("dagGcsPrefix", ""),
            })

        return {
            "project": settings.COMPOSER_PROJECT_ID,
            "location": loc,
            "environments": results,
            "total": len(results),
            "columns": ["name", "location", "state", "airflow_uri", "gcs_dag_prefix"],
            "rows": [[r["name"], r["location"], r["state"], r["airflow_uri"], r["gcs_dag_prefix"]] for r in results],
        }
    except Exception as exc:
        log.warning("list_composer_environments error: %s", exc)
        return {"error": str(exc)}


def get_composer_environment() -> dict[str, Any]:
    """Return metadata for the configured Composer V3 environment."""
    if err := _requires_composer():
        return {"error": err}
    # Fast-path: if user supplied the bucket explicitly, skip the API call
    if settings.COMPOSER_DAG_BUCKET:
        return {
            "composer_environment": settings.COMPOSER_ENVIRONMENT,
            "project": settings.COMPOSER_PROJECT_ID,
            "location": settings.COMPOSER_LOCATION,
            "gcs_dag_bucket": settings.COMPOSER_DAG_BUCKET,
            "source": "COMPOSER_DAG_BUCKET override",
        }
    try:
        from google.cloud.orchestration.airflow.service_v1 import EnvironmentsClient
        client = EnvironmentsClient()
        env = client.get_environment(name=_environment_resource_name())
        gcs_prefix = env.config.dag_gcs_prefix  # "gs://bucket/dags"
        return {
            "composer_environment": settings.COMPOSER_ENVIRONMENT,
            "project": settings.COMPOSER_PROJECT_ID,
            "location": settings.COMPOSER_LOCATION,
            "state": env.state.name,
            "airflow_uri": env.config.airflow_uri,
            "gcs_dag_prefix": gcs_prefix,
        }
    except Exception as exc:
        return {"error": str(exc)}


def _get_dag_location() -> tuple[str, str]:
    """
    Return *(bucket_name, dags_prefix)* for the configured Composer environment.
    Result is cached after the first call.
    """
    global _dag_location
    if _dag_location:
        return _dag_location

    # Explicit override
    if settings.COMPOSER_DAG_BUCKET:
        _dag_location = (settings.COMPOSER_DAG_BUCKET, "dags")
        return _dag_location

    # Discover via Composer API
    info = get_composer_environment()
    if "error" in info:
        raise RuntimeError(info["error"])

    gcs_prefix: str = info.get("gcs_dag_prefix", "")
    # gcs_dag_prefix format: "gs://bucket-name/dags"
    parts = gcs_prefix.replace("gs://", "").split("/", 1)
    bucket = parts[0]
    prefix = parts[1].rstrip("/") if len(parts) > 1 else "dags"
    _dag_location = (bucket, prefix)
    return _dag_location


def list_dag_files() -> dict[str, Any]:
    """List all DAG Python files (.py) in the Composer environment's GCS DAGs folder."""
    if err := _requires_composer():
        if not settings.COMPOSER_DAG_BUCKET:
            return {"error": err}
    try:
        import gcs_service
        bucket, prefix = _get_dag_location()
        result = gcs_service.list_blobs(bucket, prefix=prefix + "/", suffix=".py")
        result["composer_environment"] = settings.COMPOSER_ENVIRONMENT
        result["dag_gcs_prefix"] = f"gs://{bucket}/{prefix}"
        return result
    except Exception as exc:
        return {"error": str(exc)}


def read_dag_file(dag_file_path: str) -> dict[str, Any]:
    """
    Read the content of a DAG file from the Composer GCS bucket.

    Args:
        dag_file_path: blob path as returned by list_dag_files()
                       e.g. "dags/my_pipeline_dag.py"
    """
    try:
        import gcs_service
        bucket, _ = _get_dag_location()
        return gcs_service.read_blob(bucket, dag_file_path)
    except Exception as exc:
        return {"error": str(exc)}


def find_dag_for_table(table_name: str) -> dict[str, Any]:
    """
    Search all DAG files for references to *table_name*.

    Strategy:
      1. Name-based shortlist: DAG files whose filename contains a normalized form of table_name
      2. Content search: scan those files (then others if none found) for the table name
      3. Return DAG ID, file path, GCS URI, and up to 3 context snippets per match
    """
    if err := _requires_composer():
        if not settings.COMPOSER_DAG_BUCKET:
            return {"error": err}
    try:
        import gcs_service
        bucket, _ = _get_dag_location()

        listing = list_dag_files()
        if "error" in listing:
            return listing

        all_files: list[str] = listing.get("files", [])
        if not all_files:
            return {"message": "No DAG files found in the Composer bucket"}

        # Build name variants to search for
        snake = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", table_name)
        snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", snake).lower()
        variants = sorted(
            {table_name, table_name.lower(), table_name.upper(), snake, snake.replace("_", "")},
            key=len, reverse=True,  # longest first so more specific patterns match first
        )

        def _name_score(path: str) -> int:
            stem = path.split("/")[-1].replace(".py", "").lower()
            return 1 if any(v.lower() in stem for v in variants) else 0

        # Prioritise name-matched files; still scan all to be thorough
        prioritised = sorted(all_files, key=_name_score, reverse=True)

        matches: list[dict[str, Any]] = []
        for dag_path in prioritised:
            content_result = gcs_service.read_blob(bucket, dag_path)
            if "error" in content_result:
                continue
            content: str = content_result["content"]

            found = [v for v in variants if v in content]
            if not found:
                continue

            # Extract up to 3 context windows (±2 lines around first occurrence of each variant)
            lines = content.splitlines()
            snippets: list[str] = []
            for variant in found[:3]:
                for i, line in enumerate(lines):
                    if variant in line:
                        window = lines[max(0, i - 2): i + 3]
                        snippets.append("\n".join(window))
                        break

            # Try to extract dag_id from file content
            dag_id_match = re.search(r'dag_id\s*=\s*["\']([^"\']+)["\']', content)
            dag_id = (
                dag_id_match.group(1) if dag_id_match
                else dag_path.split("/")[-1].removesuffix(".py")
            )

            # Extract task IDs that look related to the table
            task_ids = re.findall(
                r'task_id\s*=\s*["\']([^"\']*' + re.escape(table_name.lower()) + r'[^"\']*)["\']',
                content,
                re.IGNORECASE,
            )

            matches.append({
                "dag_id": dag_id,
                "dag_file": dag_path,
                "gcs_uri": f"gs://{bucket}/{dag_path}",
                "matched_variants": found,
                "related_task_ids": task_ids,
                "context_snippets": snippets,
            })

        if not matches:
            return {
                "message": f"No DAG references found for '{table_name}'",
                "searched_files": len(all_files),
                "hint": "Try list_dag_files() then read_dag_file() to inspect manually",
            }

        return {
            "table_name": table_name,
            "matches": matches,
            "total_dags_searched": len(all_files),
            "total_matches": len(matches),
        }
    except Exception as exc:
        return {"error": str(exc)}
