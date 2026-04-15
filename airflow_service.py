"""
Airflow REST API service: extract compiled/rendered SQL from Composer V3 DAG task instances.

The Airflow REST API returns template fields *after* Jinja rendering, giving us the
exact SQL string that Airflow passed to BigQuery — variables, macros, and params resolved.

Authentication uses Application Default Credentials with the cloud-platform scope.
The Airflow URI is auto-discovered from the Composer environment via composer_service.
"""
import logging
import re
from typing import Any

import settings

log = logging.getLogger(__name__)

# Cached Airflow base URI for the configured environment
_airflow_uri: str | None = None

# Operator classes known to carry SQL in their template fields
_SQL_OPERATORS = frozenset({
    "BigQueryInsertJobOperator",
    "BigQueryOperator",
    "BigQueryExecuteQueryOperator",
    "SQLExecuteQueryOperator",
    "BigQueryToGCSOperator",
    "BigQueryCreateExternalTableOperator",
    "HiveOperator",
    "PrestoToGCSOperator",
    "SnowflakeOperator",
    "MSSQLOperator",
    "MySqlOperator",
    "PostgresOperator",
    "OracleOperator",
    "JdbcOperator",
})

# Template field names that are expected to hold SQL
_SQL_FIELD_NAMES = frozenset({"sql", "query", "hql", "statement"})


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get_airflow_uri() -> str:
    """Resolve and cache the Airflow web server URI for the configured environment."""
    global _airflow_uri
    if _airflow_uri:
        return _airflow_uri
    import composer_service
    info = composer_service.get_composer_environment()
    if "error" in info:
        raise RuntimeError(f"Cannot resolve Airflow URI: {info['error']}")
    uri = info.get("airflow_uri", "")
    if not uri:
        raise RuntimeError(
            "airflow_uri not returned by the Composer API. "
            "Ensure COMPOSER_ENVIRONMENT is set and the environment is RUNNING."
        )
    _airflow_uri = uri.rstrip("/")
    return _airflow_uri


def _authenticated_session():
    """Return a requests.Session carrying a fresh GCP Bearer token."""
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


def _api_get(path: str) -> dict[str, Any]:
    """
    Perform a GET against /api/v1/<path> on the Airflow web server.
    Returns the parsed JSON body or {"error": ...} on failure.
    """
    try:
        base = _get_airflow_uri()
        url = f"{base}/api/v1/{path.lstrip('/')}"
        log.debug("Airflow API GET: %s", url)
        resp = _authenticated_session().get(url, timeout=settings.AIRFLOW_API_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        log.warning("Airflow API error (%s): %s", path, exc)
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# SQL extraction helpers
# ---------------------------------------------------------------------------

def _looks_like_sql(value: str) -> bool:
    """Heuristic: does this string look like a SQL statement?"""
    s = value.strip().upper()
    return any(s.startswith(kw) for kw in (
        "SELECT", "WITH", "INSERT", "CREATE", "MERGE", "UPDATE", "DELETE", "CALL",
    ))


def _walk_for_sql(value: Any, path: str = "") -> list[dict[str, str]]:
    """
    Recursively walk a rendered_fields payload and collect every SQL-looking string.

    Returns a list of {"field_path": ..., "sql": ...} dicts ordered by discovery.
    SQL fields with names in _SQL_FIELD_NAMES are moved to the front of the list.
    """
    hits: list[dict[str, str]] = []

    if isinstance(value, str):
        if _looks_like_sql(value):
            hits.append({"field_path": path or "root", "sql": value})

    elif isinstance(value, dict):
        # Prioritise known SQL field names so primary_sql is the real query
        ordered = sorted(value.items(), key=lambda kv: (kv[0] not in _SQL_FIELD_NAMES, kv[0]))
        for key, child in ordered:
            child_path = f"{path}.{key}" if path else key
            hits.extend(_walk_for_sql(child, child_path))

    elif isinstance(value, list):
        for idx, item in enumerate(value):
            hits.extend(_walk_for_sql(item, f"{path}[{idx}]"))

    return hits


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def list_airflow_dags(limit: int = 100) -> dict[str, Any]:
    """
    List all DAGs registered in the Airflow environment.

    Returns dag_id, paused state, tags, and description for each DAG.
    """
    raw = _api_get(f"dags?limit={limit}&order_by=dag_id")
    if "error" in raw:
        return raw
    dags = [
        {
            "dag_id": d["dag_id"],
            "is_paused": d.get("is_paused", False),
            "tags": [t["name"] for t in d.get("tags", [])],
            "description": d.get("description") or "",
        }
        for d in raw.get("dags", [])
    ]
    return {"dags": dags, "total": raw.get("total_entries", len(dags))}


def list_dag_tasks(dag_id: str) -> dict[str, Any]:
    """
    List all tasks in a DAG with their operator class and template fields.
    Each task entry includes a `likely_has_sql` flag for quick filtering.

    Args:
        dag_id: the DAG identifier
    """
    raw = _api_get(f"dags/{dag_id}/tasks")
    if "error" in raw:
        return raw

    tasks = []
    for t in raw.get("tasks", []):
        class_ref = t.get("class_ref", {})
        operator = class_ref.get("class_name") or t.get("operator_name", "")
        template_fields: list[str] = t.get("template_fields", [])
        likely_sql = (
            operator in _SQL_OPERATORS
            or bool(_SQL_FIELD_NAMES & set(template_fields))
        )
        tasks.append({
            "task_id": t["task_id"],
            "operator_class": operator,
            "template_fields": template_fields,
            "likely_has_sql": likely_sql,
        })

    return {"dag_id": dag_id, "tasks": tasks, "total": len(tasks)}


def get_dag_runs(dag_id: str, limit: int = 5) -> dict[str, Any]:
    """
    Return the most recent DAG runs for a given DAG (ordered newest first).
    A run_id from here is required by get_rendered_task_sql().

    Args:
        dag_id: the DAG identifier
    """
    raw = _api_get(f"dags/{dag_id}/dagRuns?limit={limit}&order_by=-execution_date")
    if "error" in raw:
        return raw
    runs = [
        {
            "dag_run_id": r["dag_run_id"],
            "state": r.get("state", ""),
            "execution_date": r.get("execution_date", ""),
            "start_date": r.get("start_date", ""),
            "end_date": r.get("end_date", ""),
        }
        for r in raw.get("dag_runs", [])
    ]
    return {
        "dag_id": dag_id,
        "runs": runs,
        "hint": "Pass a dag_run_id with state='success' to get_rendered_task_sql().",
    }


def find_sql_tasks_in_dag(dag_id: str) -> dict[str, Any]:
    """
    Identify tasks in a DAG that likely contain SQL, using static task metadata.
    No dag_run_id is needed — use this as the first step to know which tasks to extract.

    Args:
        dag_id: the DAG identifier
    """
    result = list_dag_tasks(dag_id)
    if "error" in result:
        return result

    all_tasks: list[dict] = result.get("tasks", [])
    sql_tasks = [t for t in all_tasks if t["likely_has_sql"]]

    return {
        "dag_id": dag_id,
        "sql_tasks": sql_tasks,
        "sql_task_count": len(sql_tasks),
        "total_task_count": len(all_tasks),
        "next_step": (
            "Call get_dag_runs(dag_id) to obtain a dag_run_id, "
            "then call get_rendered_task_sql(dag_id, task_id, dag_run_id) for each sql task."
        ),
    }


def get_dag_source(dag_id: str) -> dict[str, Any]:
    """
    Retrieve the Python source code of a DAG by its dag_id.

    Strategy:
      1. Call GET /api/v1/dags/{dag_id} → extract fileloc (server-side path)
      2. Strip the Composer GCS mount prefix (/home/airflow/gcs/) → blob path
      3. Read the blob from the Composer DAG GCS bucket

    This is the primary entry point for DAG code optimization: no need to know the
    file path in advance — just provide the dag_id.

    Args:
        dag_id: the DAG identifier as shown in the Airflow UI
    """
    dag_info = _api_get(f"dags/{dag_id}")
    if "error" in dag_info:
        return dag_info

    fileloc: str = dag_info.get("fileloc", "")
    if not fileloc:
        return {
            "error": f"fileloc not returned for DAG '{dag_id}' — the DAG may not be synced yet.",
            "dag_info": dag_info,
        }

    # Composer V3 mounts the GCS DAGs bucket at /home/airflow/gcs/ on every worker.
    # Strip that prefix to get the GCS blob path (e.g. "dags/subdir/my_dag.py").
    blob_path = re.sub(r"^/home/airflow/gcs/", "", fileloc)

    try:
        import composer_service as _composer
        import gcs_service as _gcs
        bucket, _ = _composer._get_dag_location()
        result = _gcs.read_blob(bucket, blob_path)
        result["dag_id"] = dag_id
        result["fileloc"] = fileloc
        result["citation"] = f"[DAG: {dag_id} | gs://{bucket}/{blob_path}]"
        return result
    except Exception as exc:
        return {"error": str(exc), "dag_id": dag_id, "fileloc": fileloc, "blob_path": blob_path}


def get_rendered_task_sql(dag_id: str, task_id: str, dag_run_id: str) -> dict[str, Any]:
    """
    Fetch the compiled/rendered SQL from a specific Airflow task instance.

    This calls the Airflow REST API renderedFields endpoint which returns template
    fields *after* Jinja rendering — i.e., the exact SQL string that was executed.

    Args:
        dag_id:     the DAG identifier
        task_id:    the task identifier within the DAG
        dag_run_id: a dag_run_id from get_dag_runs() (prefer state='success')

    Returns:
        primary_sql: the main SQL string extracted from the rendered fields
        sql_fields:  all SQL-looking strings found, with their field paths
        citation:    formatted source reference for use in audit reports
    """
    path = (
        f"dags/{dag_id}/dagRuns/{dag_run_id}"
        f"/taskInstances/{task_id}/renderedFields"
    )
    raw = _api_get(path)
    if "error" in raw:
        return raw

    rendered: dict = raw.get("rendered_fields", {})
    sql_hits = _walk_for_sql(rendered)

    if not sql_hits:
        return {
            "dag_id": dag_id,
            "task_id": task_id,
            "dag_run_id": dag_run_id,
            "message": "No SQL-like content found in rendered template fields.",
            "rendered_fields_keys": list(rendered.keys()),
            "hint": (
                "The operator may store SQL under a non-standard field name, "
                "or the SQL may be loaded from a file reference (e.g. {% include %})."
            ),
        }

    citation = f"[DAG: {dag_id} | Task: {task_id} | Run: {dag_run_id}]"
    return {
        "dag_id": dag_id,
        "task_id": task_id,
        "dag_run_id": dag_run_id,
        "primary_sql": sql_hits[0]["sql"],
        "sql_fields": sql_hits,
        "citation": citation,
        "note": (
            "primary_sql is the first SQL-like string found — typically the main query. "
            "Check sql_fields if multiple SQL strings were detected."
        ),
    }
