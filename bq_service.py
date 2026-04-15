import datetime
import decimal
import logging
from typing import Any

from google.cloud import bigquery
import settings

log = logging.getLogger(__name__)
client: bigquery.Client | None = None


def get_client() -> bigquery.Client:
    global client
    if client is None:
        client = bigquery.Client(project=settings.BQ_BILLING_PROJECT)
    return client


def _check_project(project: str) -> str | None:
    if project not in settings.BQ_PROJECTS:
        return f"Project '{project}' is not allowed. Allowed: {', '.join(settings.BQ_PROJECTS)}"
    return None


def list_datasets(project: str) -> dict[str, Any]:
    if err := _check_project(project):
        return {"error": err}
    try:
        datasets = [ds.dataset_id for ds in get_client().list_datasets(project=project)]
        return {"project": project, "datasets": datasets}
    except Exception as exc:
        return {"error": str(exc)}


def list_tables(project: str, dataset: str) -> dict[str, Any]:
    if err := _check_project(project):
        return {"error": err}
    try:
        tables = [t.table_id for t in get_client().list_tables(f"{project}.{dataset}")]
        return {"project": project, "dataset": dataset, "tables": tables}
    except Exception as exc:
        return {"error": str(exc)}


def get_table_schema(project: str, dataset: str, table: str) -> dict[str, Any]:
    if err := _check_project(project):
        return {"error": err}
    try:
        bq = get_client()
        ref = bq.get_table(f"{project}.{dataset}.{table}")
        import json
        schema = [{"column": f.name, "type": f.field_type, "mode": f.mode} for f in ref.schema]
        row_count = next(bq.query(f"SELECT COUNT(*) FROM `{project}.{dataset}.{table}`").result())[0]
        fqn = f"{project}.{dataset}.{table}"
        formatted = json.dumps({fqn: schema, "row_count": row_count}, indent=2)
        return {"result": formatted}
    except Exception as exc:
        return {"error": str(exc)}


def execute_sql(sql: str) -> dict[str, Any]:
    sanitised = sql.strip().rstrip(";")
    upper = sanitised.upper()
    for kw in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE"):
        if upper.startswith(kw) or f" {kw} " in upper:
            return {"error": f"Write operations are not allowed: {kw}"}
    try:
        result = get_client().query(sanitised).result()
        columns = [f.name for f in result.schema]
        rows = [list(row.values()) for row in result]
        if settings.MAX_SQL_ROWS > 0:
            truncated = len(rows) > settings.MAX_SQL_ROWS
            rows = rows[:settings.MAX_SQL_ROWS]
        else:
            truncated = False
        return {
            "columns": columns,
            "rows": [[_to_python(v) for v in row] for row in rows],
            "row_count": len(rows),
            "truncated": truncated,
        }
    except Exception as exc:
        log.warning("SQL error: %s — %s", exc, sanitised[:200])
        return {"error": str(exc)}


def dry_run_sql(sql: str) -> dict[str, Any]:
    """
    Submit *sql* as a BigQuery dry-run job and return cost / size estimates.
    No data is read; BigQuery computes the estimate from metadata only.
    """
    sanitised = sql.strip().rstrip(";")
    try:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = get_client().query(sanitised, job_config=job_config)
        bytes_processed: int = job.total_bytes_processed or 0
        tb = bytes_processed / 1e12
        cost_usd = round(tb * 6.25, 4)
        return {
            "bytes_processed": bytes_processed,
            "bytes_processed_human": _human_bytes(bytes_processed),
            "estimated_cost_usd": cost_usd,
            "pricing_note": "BigQuery on-demand: $6.25 / TB scanned",
        }
    except Exception as exc:
        return {"error": str(exc)}


def _human_bytes(n: int) -> str:
    """Format a byte count as a human-readable string (B / KB / MB / GB / TB)."""
    for unit, threshold in (("TB", 1e12), ("GB", 1e9), ("MB", 1e6), ("KB", 1e3)):
        if n >= threshold:
            return f"{n / threshold:.2f} {unit}"
    return f"{n} B"


def _sql_stats(sql: str) -> dict[str, int]:
    """
    Static structural analysis of a SQL string.
    Returns counts that serve as complexity / refactor-quality metrics.
    """
    import re
    text = sql

    # Strip single-line and block comments before counting
    text_no_comments = re.sub(r"--[^\n]*", " ", text)
    text_no_comments = re.sub(r"/\*.*?\*/", " ", text_no_comments, flags=re.DOTALL)

    upper = text_no_comments.upper()

    # CTE definitions: WITH <name> AS (
    cte_count = len(re.findall(r"\bWITH\b[\s\S]*?\bAS\s*\(", text_no_comments, re.IGNORECASE))
    # A simpler proxy: count "AS (" occurrences at the WITH level
    # Use a lookahead-free approach: count standalone "AS (" not preceded by non-whitespace (i.e., in CTE position)
    cte_count = len(re.findall(r"(?:,|\bWITH\b)\s+\w+\s+AS\s*\(", text_no_comments, re.IGNORECASE))

    # Subqueries: count SELECT occurrences inside parentheses (total SELECT minus 1 for the outermost)
    select_count = len(re.findall(r"\bSELECT\b", upper))
    subquery_count = max(select_count - 1, 0)

    # JOINs
    join_count = len(re.findall(r"\bJOIN\b", upper))

    # Maximum parenthesis nesting depth
    depth = max_depth = 0
    for ch in text_no_comments:
        if ch == "(":
            depth += 1
            max_depth = max(max_depth, depth)
        elif ch == ")":
            depth = max(depth - 1, 0)

    return {
        "line_count": len(sql.splitlines()),
        "cte_count": cte_count,
        "subquery_count": subquery_count,
        "join_count": join_count,
        "max_nesting_depth": max_depth,
    }


def estimate_sql_cost(sql_text: str) -> dict[str, Any]:
    """
    Return BigQuery dry-run cost estimate **and** static structural stats for *sql_text*.
    Use this on both original and optimized SQL to populate the Performance Impact Report.
    """
    stats = _sql_stats(sql_text)
    dry = dry_run_sql(sql_text)
    return {**dry, "stats": stats}


def _to_python(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, decimal.Decimal):
        return round(float(val), 2)
    if isinstance(val, float):
        return round(val, 2)
    if isinstance(val, (datetime.date, datetime.datetime, datetime.time)):
        return val.isoformat()
    if isinstance(val, datetime.timedelta):
        return str(val)
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return val
