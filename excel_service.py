import datetime
import decimal
import logging
import re
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

import settings

log = logging.getLogger(__name__)

# table_name -> {file, sheet, rows, columns}
_registry: dict[str, dict[str, Any]] = {}
_conn: duckdb.DuckDBPyConnection | None = None
_loaded: bool = False


def _get_conn() -> duckdb.DuckDBPyConnection:
    global _conn
    if _conn is None:
        _conn = duckdb.connect()
    return _conn


def _make_table_name(file_path: Path, sheet_name: str, base_path: Path) -> str:
    """Derive a stable, SQL-safe table name from the relative file path + sheet."""
    rel = file_path.relative_to(base_path)
    parts = list(rel.parts[:-1]) + [rel.stem, sheet_name]
    name = "__".join(parts)
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_").lower()
    if name and name[0].isdigit():
        name = "t_" + name
    return name


def _sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten multi-level headers and make all column names SQL-safe."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join(str(c).strip() for c in col if str(c).strip() not in ("", "nan"))
            for col in df.columns
        ]
    else:
        df.columns = [str(c).strip() for c in df.columns]

    sanitized = []
    for i, col in enumerate(df.columns):
        col = re.sub(r"[^a-zA-Z0-9_]", "_", col)
        col = re.sub(r"_+", "_", col).strip("_")
        sanitized.append(col if col else f"col_{i}")
    df.columns = sanitized
    return df


def _to_python(val: Any) -> Any:
    """Convert numpy/pandas types to plain Python for JSON serialisation."""
    if val is None:
        return None
    if isinstance(val, float) and val != val:  # NaN
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return round(float(val), 4)
    if isinstance(val, (np.bool_,)):
        return bool(val)
    if isinstance(val, (datetime.date, datetime.datetime)):
        return val.isoformat()
    if isinstance(val, decimal.Decimal):
        return float(val)
    if isinstance(val, pd.Timestamp):
        return val.isoformat()
    return val


def load_excel_files() -> dict[str, Any]:
    """Scan EXCEL_DATA_PATH recursively, load every Excel sheet into DuckDB."""
    global _loaded

    if not settings.EXCEL_DATA_PATH:
        _loaded = True
        return {"loaded": 0, "message": "EXCEL_DATA_PATH not configured"}

    base_path = Path(settings.EXCEL_DATA_PATH).expanduser().resolve()
    if not base_path.exists():
        _loaded = True
        return {"error": f"EXCEL_DATA_PATH does not exist: {base_path}"}

    conn = _get_conn()
    _registry.clear()
    loaded, errors = 0, []

    header_arg: int | list[int] = (
        list(range(settings.EXCEL_HEADER_ROWS)) if settings.EXCEL_HEADER_ROWS > 1 else 0
    )

    excel_files: list[Path] = []
    for ext in ("*.xlsx", "*.xlsm", "*.xls"):
        excel_files.extend(sorted(base_path.rglob(ext)))

    for file_path in excel_files:
        try:
            sheets: dict[str, pd.DataFrame] = pd.read_excel(
                file_path, sheet_name=None, header=header_arg, engine="openpyxl"
            )
            for sheet_name, df in sheets.items():
                df = _sanitize_columns(df)
                table_name = _make_table_name(file_path, sheet_name, base_path)
                rel_path = str(file_path.relative_to(base_path))

                conn.register(table_name, df)
                _registry[table_name] = {
                    "file": rel_path,
                    "sheet": sheet_name,
                    "rows": len(df),
                    "columns": list(df.columns),
                }
                loaded += 1
                log.info(
                    "Loaded Excel: %s → sheet '%s' as table '%s' (%d rows, %d cols)",
                    rel_path, sheet_name, table_name, len(df), len(df.columns),
                )
        except Exception as exc:
            rel = str(file_path.relative_to(base_path)) if file_path.is_relative_to(base_path) else str(file_path)
            log.warning("Failed to load %s: %s", rel, exc)
            errors.append({"file": rel, "error": str(exc)})

    _loaded = True
    result: dict[str, Any] = {"loaded": loaded}
    if errors:
        result["errors"] = errors
    return result


def _ensure_loaded() -> None:
    if not _loaded:
        load_excel_files()


# ---------------------------------------------------------------------------
# Tools exposed to the agent
# ---------------------------------------------------------------------------

def list_excel_files() -> dict[str, Any]:
    """Return all Excel files that were loaded, grouped by file with their sheets."""
    _ensure_loaded()
    seen: dict[str, list[str]] = {}
    for info in _registry.values():
        seen.setdefault(info["file"], []).append(info["sheet"])
    return {
        "files": [{"file": f, "sheets": sheets} for f, sheets in seen.items()],
        "total_files": len(seen),
        "total_tables": len(_registry),
    }


def list_excel_tables() -> dict[str, Any]:
    """Return every queryable table with its columns and source citation."""
    _ensure_loaded()
    return {
        "tables": [
            {
                "table": name,
                "citation": {"file": info["file"], "sheet": info["sheet"]},
                "rows": info["rows"],
                "columns": info["columns"],
            }
            for name, info in _registry.items()
        ]
    }


def get_excel_schema(table_name: str) -> dict[str, Any]:
    """Get column list and row count for a single Excel table."""
    _ensure_loaded()
    if table_name not in _registry:
        return {
            "error": f"Table '{table_name}' not found.",
            "available_tables": list(_registry.keys()),
        }
    info = _registry[table_name]
    return {
        "table": table_name,
        "citation": {"file": info["file"], "sheet": info["sheet"]},
        "columns": info["columns"],
        "rows": info["rows"],
    }


def query_excel(sql: str) -> dict[str, Any]:
    """Execute a DuckDB SELECT against loaded Excel tables; returns rows + citations."""
    _ensure_loaded()
    sanitised = sql.strip().rstrip(";")
    upper = sanitised.upper()
    for kw in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE"):
        if upper.startswith(kw) or f" {kw} " in upper:
            return {"error": f"Write operations are not allowed: {kw}"}

    try:
        conn = _get_conn()
        cursor = conn.execute(sanitised)
        columns = [desc[0] for desc in cursor.description]
        raw_rows = cursor.fetchall()
        rows = [[_to_python(v) for v in row] for row in raw_rows]

        # Identify which registered tables appear in the SQL for citations
        citations = [
            {"table": t, "file": _registry[t]["file"], "sheet": _registry[t]["sheet"]}
            for t in _registry
            if re.search(rf"\b{re.escape(t)}\b", sanitised, re.IGNORECASE)
        ]

        truncated = False
        if settings.MAX_SQL_ROWS > 0 and len(rows) > settings.MAX_SQL_ROWS:
            rows = rows[: settings.MAX_SQL_ROWS]
            truncated = True

        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated,
            "citations": citations,
        }
    except Exception as exc:
        log.warning("Excel SQL error: %s — %s", exc, sanitised[:200])
        return {"error": str(exc)}
