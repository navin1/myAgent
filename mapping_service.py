"""
Mapping service: semantic parsing of Excel transformation specification files.

Classification is folder-based (not filename-based):
  - Mapping Sheets : files inside EXCEL_MAPPING_PATH (and its subfolders)
                     multi-row header (EXCEL_MAPPING_HEADER_ROWS), source→target column logic
  - Master Sheets  : every other Excel file under EXCEL_DATA_PATH
                     single or multi-row header (EXCEL_MASTER_HEADER_ROWS), structural definitions

The resulting index lets the agent answer:
  "What columns does table X map from source to target?"
  "What transformation logic is defined for column Y?"
"""
import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd

import settings

log = logging.getLogger(__name__)

# Normalised-name → parsed spec
_index: dict[str, dict[str, Any]] = {}
_loaded: bool = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_name(name: str) -> str:
    """Any naming convention → lowercase_snake_case for stable lookups."""
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _classify_file(path: Path, mapping_root: Path | None) -> str:
    """
    Return 'mapping' if *path* lives inside *mapping_root*, 'master' otherwise.
    Classification is purely folder-based — filenames are irrelevant.
    """
    if mapping_root and path.is_relative_to(mapping_root):
        return "mapping"
    return "master"


def _flatten_multiindex(df: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse a MultiIndex column header into single-level strings.
    Levels are joined with " | " so origin is still readable.
    """
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " | ".join(
                str(c).strip() for c in col
                if str(c).strip() not in ("", "nan", "None")
            )
            for col in df.columns
        ]
    else:
        df.columns = [str(c).strip() for c in df.columns]
    return df


def _extract_source_target_pairs(df: pd.DataFrame) -> list[dict[str, str]]:
    """
    Heuristically locate source-column, target-column, and transformation columns
    from a flattened mapping sheet and return structured pairs.

    Detection logic (first match wins):
      - source col: column name contains both "source" (or "src") and "column" (or "col" or "field")
      - target col: column name contains both "target" (or "tgt" or "dest") and "column" (or "col" or "field")
      - transformation: column name contains any of "transform", "logic", "rule", "expression", "formula", "derivation"
    """
    cols = list(df.columns)

    def _find(keywords_a: list[str], keywords_b: list[str]) -> str | None:
        for c in cols:
            cl = c.lower()
            if any(k in cl for k in keywords_a) and any(k in cl for k in keywords_b):
                return c
        return None

    src_col = _find(["source", "src"], ["column", "col", "field", "attribute"])
    tgt_col = _find(["target", "tgt", "destination", "dest"], ["column", "col", "field", "attribute"])
    tfm_col = _find(
        ["transform", "logic", "rule", "expression", "formula", "derivation", "calculation", "mapping"],
        [""],  # any column containing these words qualifies
    )
    # Special case: single keyword match for transformation
    if not tfm_col:
        tfm_col = next(
            (c for c in cols if any(
                k in c.lower()
                for k in ("transform", "logic", "rule", "expression", "formula", "derivation")
            )),
            None,
        )

    if not src_col and not tgt_col:
        return []

    pairs: list[dict[str, str]] = []
    for _, row in df.iterrows():
        entry: dict[str, str] = {}
        if src_col and pd.notna(row.get(src_col)):
            v = str(row[src_col]).strip()
            if v:
                entry["source_column"] = v
        if tgt_col and pd.notna(row.get(tgt_col)):
            v = str(row[tgt_col]).strip()
            if v:
                entry["target_column"] = v
        if tfm_col and pd.notna(row.get(tfm_col)):
            v = str(row[tfm_col]).strip()
            if v and v.lower() not in ("nan", "none", "-"):
                entry["transformation"] = v
        if entry:
            pairs.append(entry)

    return pairs


def _parse_sheet(
    df: pd.DataFrame,
    file_type: str,
) -> dict[str, Any]:
    """Return a parsed representation of a single sheet."""
    df = _flatten_multiindex(df)

    # Trim rows that are completely empty
    df = df.dropna(how="all").reset_index(drop=True)

    sheet_info: dict[str, Any] = {
        "columns": list(df.columns),
        "row_count": len(df),
    }

    if file_type == "mapping":
        pairs = _extract_source_target_pairs(df)
        if pairs:
            sheet_info["source_target_pairs"] = pairs
            sheet_info["mapped_columns"] = len(pairs)

    # Store raw data (capped for LLM context; full data available via DuckDB query)
    records = df.where(df.notna(), None).to_dict(orient="records")
    sheet_info["sample_rows"] = records[:50]
    if len(records) > 50:
        sheet_info["sample_note"] = f"{len(records)} rows total; showing first 50. Use query_excel() for full data."

    return sheet_info


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def _header_arg(file_type: str) -> int | list[int]:
    """Return the pandas header= argument for a given file type."""
    if file_type == "mapping":
        n = settings.EXCEL_MAPPING_HEADER_ROWS
    else:
        n = settings.EXCEL_MASTER_HEADER_ROWS
    return list(range(n)) if n > 1 else 0


def load_mapping_files() -> dict[str, Any]:
    """
    Scan Excel spec files and index them by table name.

    Scanning strategy (applied in order):
      1. If both EXCEL_MASTER_PATH and EXCEL_MAPPING_PATH are set:
           - Only files inside EXCEL_MASTER_PATH  → Master Sheets
           - Only files inside EXCEL_MAPPING_PATH → Mapping Sheets
           - Files outside both folders are skipped
      2. If only EXCEL_MAPPING_PATH is set (no EXCEL_MASTER_PATH):
           - Files inside EXCEL_MAPPING_PATH → Mapping Sheets
           - All other files under EXCEL_DATA_PATH → Master Sheets
      3. If neither path is set but EXCEL_DATA_PATH is set:
           - All files under EXCEL_DATA_PATH → Master Sheets

    Indexes every file by its normalised table name (= file stem without extension).
    """
    global _loaded

    if not settings.EXCEL_DATA_PATH:
        _loaded = True
        return {"loaded": 0, "message": "EXCEL_DATA_PATH not configured"}

    base_path = Path(settings.EXCEL_DATA_PATH).expanduser().resolve()
    if not base_path.exists():
        _loaded = True
        return {"error": f"EXCEL_DATA_PATH does not exist: {base_path}"}

    # Resolve the optional master and mapping folders once
    master_root: Path | None = None
    if settings.EXCEL_MASTER_PATH:
        master_root = Path(settings.EXCEL_MASTER_PATH).expanduser().resolve()
        if not master_root.exists():
            log.warning("EXCEL_MASTER_PATH does not exist: %s — ignoring", master_root)
            master_root = None

    mapping_root: Path | None = None
    if settings.EXCEL_MAPPING_PATH:
        mapping_root = Path(settings.EXCEL_MAPPING_PATH).expanduser().resolve()
        if not mapping_root.exists():
            log.warning("EXCEL_MAPPING_PATH does not exist: %s — treating all files as master", mapping_root)
            mapping_root = None

    # Build the candidate file list
    explicit_roots = bool(master_root or mapping_root)
    _index.clear()
    loaded, skipped, errors = 0, 0, []

    excel_files: list[Path] = []
    for ext in ("*.xlsx", "*.xlsm", "*.xls"):
        excel_files.extend(sorted(base_path.rglob(ext)))

    for file_path in excel_files:
        # Determine type — skip files outside both explicit roots when both are configured
        if master_root and mapping_root:
            if file_path.is_relative_to(mapping_root):
                file_type = "mapping"
            elif file_path.is_relative_to(master_root):
                file_type = "master"
            else:
                log.debug("Skipping %s — outside both EXCEL_MASTER_PATH and EXCEL_MAPPING_PATH", file_path)
                skipped += 1
                continue
        else:
            file_type = _classify_file(file_path, mapping_root)

        header_arg = _header_arg(file_type)

        try:
            sheets: dict[str, pd.DataFrame] = pd.read_excel(
                file_path, sheet_name=None, header=header_arg, engine="openpyxl"
            )
            rel_path = str(file_path.relative_to(base_path))
            table_name = file_path.stem  # filename (without extension) = table name
            norm_key = _normalize_name(table_name)

            parsed_sheets = {
                sheet_name: _parse_sheet(df, file_type)
                for sheet_name, df in sheets.items()
            }

            _index[norm_key] = {
                "table_name": table_name,
                "normalized_name": norm_key,
                "file": rel_path,
                "type": file_type,
                "sheets": parsed_sheets,
            }
            loaded += 1
            log.info(
                "Indexed %s spec: %s → '%s' (%d sheet(s))",
                file_type, rel_path, table_name, len(parsed_sheets),
            )
        except Exception as exc:
            rel = str(file_path.relative_to(base_path)) if file_path.is_relative_to(base_path) else str(file_path)
            log.warning("Failed to parse spec file %s: %s", rel, exc)
            errors.append({"file": rel, "error": str(exc)})

    _loaded = True
    result: dict[str, Any] = {
        "loaded": loaded,
        "master": sum(1 for v in _index.values() if v["type"] == "master"),
        "mapping": sum(1 for v in _index.values() if v["type"] == "mapping"),
        "master_folder": str(master_root) if master_root else "not configured",
        "mapping_folder": str(mapping_root) if mapping_root else "not configured (all files treated as master)",
    }
    if skipped:
        result["skipped"] = skipped
    if errors:
        result["errors"] = errors
    return result


def _ensure_loaded() -> None:
    if not _loaded:
        load_mapping_files()


# ---------------------------------------------------------------------------
# Agent-facing tools
# ---------------------------------------------------------------------------

def list_mapping_tables() -> dict[str, Any]:
    """
    List all tables found in Excel Master and Mapping spec files.
    Returns table name, spec type, source file, available sheets,
    and (for mapping files) the count of source→target pairs.
    """
    _ensure_loaded()
    tables = []
    for v in _index.values():
        entry: dict[str, Any] = {
            "table_name": v["table_name"],
            "type": v["type"],
            "file": v["file"],
            "sheets": list(v["sheets"].keys()),
        }
        if v["type"] == "mapping":
            total_pairs = sum(
                s.get("mapped_columns", 0) for s in v["sheets"].values()
            )
            entry["total_source_target_pairs"] = total_pairs
        tables.append(entry)

    return {"tables": tables, "total": len(tables)}


def get_table_mapping(table_name: str) -> dict[str, Any]:
    """
    Retrieve the full Excel specification for a table.

    Returns:
      - file & type (master/mapping)
      - per-sheet column list, row count, source→target pairs, and sample rows
      - a 'citation' key for use in audit reports

    Args:
        table_name: the table name to look up (exact or approximate match)
    """
    _ensure_loaded()
    norm = _normalize_name(table_name)

    # 1. Exact match
    if norm in _index:
        entry = _index[norm]
        return {**entry, "citation": f"[{entry['file']} — {entry['type']} spec]"}

    # 2. Substring match (longest key that overlaps)
    candidates = [k for k in _index if norm in k or k in norm]
    if candidates:
        best = max(candidates, key=lambda k: len(set(norm.split("_")) & set(k.split("_"))))
        entry = _index[best]
        return {
            **entry,
            "citation": f"[{entry['file']} — {entry['type']} spec]",
            "note": f"Closest match for '{table_name}' → '{entry['table_name']}'",
        }

    return {
        "error": f"No spec file found matching '{table_name}'",
        "available_tables": [v["table_name"] for v in _index.values()],
        "hint": "Call list_mapping_tables() to see all available specs",
    }
