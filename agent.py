import logging
import re
from typing import Any

from google.genai import types
from google.protobuf.json_format import MessageToDict

import settings
import bq_service
import gcs_service

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# System prompt – composed from enabled capability sections
# ---------------------------------------------------------------------------

_BQ_SECTION = f"""## BigQuery
Projects: {", ".join(settings.BQ_PROJECTS)}
Tools:
- list_datasets(project): list datasets in a project
- list_tables(project, dataset): list tables in a dataset
- get_table_schema(project, dataset, table): get columns, types, and row count
- execute_sql(sql_query): run a SELECT; use fully-qualified `project.dataset.table` names
- estimate_sql_cost(sql_text): dry-run a SQL string against BigQuery (no data read) and return
  bytes_processed, estimated_cost_usd, and structural stats (line_count, cte_count,
  subquery_count, join_count, max_nesting_depth). Use on both original and optimized SQL
  during tuning to populate the Performance Impact Report.

Rules:
- For schema questions call get_table_schema — never use execute_sql for this.
- If the user provides a SQL statement, pass it directly to execute_sql unchanged.
- For `project.dataset.table` references, use the parts directly without exploring.
- Cross-project JOINs are supported. Only SELECT is allowed."""

_EXCEL_SECTION = """
## Excel data (DuckDB)
Tools:
- list_excel_files(): list all loaded Excel files and their sheets
- list_excel_tables(): list all queryable tables with column names (call before querying)
- get_excel_schema(table_name): get columns and row count for one table
- query_excel(sql_query): DuckDB SQL SELECT against Excel tables; cross-file JOINs supported

Rules:
- Always call list_excel_tables() before querying if table names are unknown.
- Every response containing Excel data MUST cite sources as: [filename.xlsx → SheetName]
- The query result includes a `citations` field — include ALL of them in your response.
- Only SELECT is allowed.""" if settings.EXCEL_DATA_PATH else ""

_MAPPING_SECTION = """
## Excel Mapping Specs
Two spec types are indexed:
  - Master Sheets  : structural/reference definitions (1-row header)
  - Mapping Sheets : source→target column transformation logic (multi-row header)

Tools:
- list_mapping_tables(): list all tables that have a Master or Mapping spec file
- get_table_mapping(table_name): retrieve the full spec — source/target pairs, transformation
  rules, and all raw sheet data for a table

Rules:
- Always cite the spec file in responses: [filename.xlsx — mapping spec]
- source_target_pairs contains the ground truth for what the SQL must implement.""" if settings.EXCEL_DATA_PATH else ""

_SCHEMA_AUDIT_SECTION = f"""
## Schema Audit (MySQL → BigQuery reconciliation)
Metadata project: {settings.SCHEMA_METADATA_PROJECT}

Tool:
- run_schema_audit(): reconcile MySQL source columns against live BigQuery view schemas.
  Reads streamed tables from HEADER_VIEW/DETAIL_VIEW, fetches BQ schemas, and writes:
    • schema_audit_<timestamp>_prd.xlsx  — prod tables (deployed_to_prod=1)
    • schema_audit_<timestamp>_uat.xlsx  — UAT tables
    • schema_ddl_<timestamp>_prd.json   — BQ DDL for prod tables
    • schema_ddl_<timestamp>_uat.json   — BQ DDL for UAT tables
  Output files are written to SCHEMA_AUDIT_OUTPUT_DIR (configured in .env).

Status legend in the Excel report:
  🟢 Match       — MySQL and BQ types are equivalent
  🟡 Type Mismatch — types differ
  🟠 BQ Only     — column exists in BQ but not in MySQL metadata
  🔵 MySQL Only  — column exists in MySQL metadata but not in BQ

Rules:
- Call run_schema_audit() directly — no arguments needed.
- Report the output file paths and the column-level counts from the result.""" if settings.SCHEMA_METADATA_PROJECT else ""

_GCS_SECTION = """
## GCS File Access
Tool (always available):
- read_gcs_path(gcs_uri): read any GCS file via its full gs://bucket/path URI""" + ("""

## GCS SQL Scripts
Tools:
- list_sql_files(prefix?): browse .sql files in the configured SQL GCS bucket
- find_sql_for_table(table_name): locate and read the SQL file for a table (fuzzy match)
- read_sql_file(path): read a specific .sql file by its GCS blob path

Rules:
- Always cite SQL files as: [gs://bucket/path/file.sql]
- Use find_sql_for_table() first; fall back to list_sql_files() + read_sql_file() if needed.""" if settings.SQL_GCS_BUCKET else "")

_AUDIT_SECTION = """
## Transformation Audit Workflow
When the user asks to audit, trace, validate, or compare a table:

Step 1 — Gather specs:     call get_table_mapping(table_name)
Step 2 — Locate SQL:       call find_sql_for_table(table_name)
Step 3 — Analyse & report using this exact structure:

---
### Audit Report: <TableName>

**Traceability**
| Layer | Reference |
|---|---|
| Excel Spec | [file.xlsx — mapping spec] |
| SQL File   | [gs://bucket/path/file.sql] |

**Mapping Verification**
| Source Column | Target Column | Excel Rule | SQL Implementation | Status |
|---|---|---|---|---|
| src_col | tgt_col | CASE ... | CASE ... END | ✓ Match / ✗ Mismatch / ⚠ Partial |

**Discrepancies**
<Numbered list of every mismatch — column name differences, missing CASE branches,
wrong join conditions, hardcoded values not in spec, etc.>

**Optimization Suggestions**
<Numbered list of SQL inefficiencies: redundant joins, full-table scans, repeated subqueries, etc.>

**Citations**
- Excel: [file.xlsx — mapping spec]
- SQL:   [gs://bucket/path/file.sql]
---""" if settings.EXCEL_DATA_PATH or settings.SQL_GCS_BUCKET else ""

_TUNING_SECTION = """
## BigQuery SQL Performance Tuning
Triggered when the user asks to tune, optimize, refactor, or analyze a SQL file from GCS.

### Workflow (follow every step in order)

Step 1 — Read the file
  Call read_gcs_path(gcs_uri) with the exact gs:// URI provided by the user.
  Capture the full SQL text as `original_sql`.

Step 2 — Baseline the original
  Call estimate_sql_cost(sql_text=original_sql).
  Capture bytes_processed, estimated_cost_usd, and all stats fields.
  This is the BEFORE measurement.

Step 3 — Analyze
  Examine every clause for the patterns listed below.

Step 4 — Rewrite
  Apply all SAFE changes (see categories) to produce `optimized_sql`.

Step 5 — Measure the optimized version
  Call estimate_sql_cost(sql_text=optimized_sql).
  Capture bytes_processed, estimated_cost_usd, and stats.
  This is the AFTER measurement.

Step 6 — Produce the report
  Use the exact format specified at the end of this section.
  Populate the Performance Impact Report from the BEFORE and AFTER measurements.

### Pattern categories

SAFE TO REWRITE (always preserves functional parity — apply automatically):
- Repeated subquery → extract into a named CTE used in place of each reference
- NOT IN (SELECT …) → LEFT JOIN … WHERE right_key IS NULL  (avoids NULL-propagation pitfalls and full scans)
- Correlated subquery in SELECT or WHERE → rewrite as JOIN or LATERAL JOIN
- HAVING clause with no aggregate reference → move condition to WHERE (filters before grouping)
- ORDER BY inside a CTE or subquery → remove entirely (BigQuery ignores it and wastes sort resources)
- CASE WHEN x THEN y ELSE NULL END → IF(x, y, NULL)
- CASE WHEN x IS NULL THEN y ELSE x END → COALESCE(x, y)
- CASE WHEN x THEN TRUE ELSE FALSE END → cast/boolean expression of x directly
- Subquery wrapping a window function solely to apply a filter → QUALIFY clause
- IN (literal1, literal2, …) with > 10 literals → UNNEST([…]) join (enables pruning)
- EXISTS vs IN for existence check against a large subquery → rewrite as EXISTS

FLAG ONLY — do not rewrite (may alter results; require human review):
- SELECT * → list explicit columns (flag: bytes scanned unknown without schema)
- UNION → UNION ALL: flag if rows are provably distinct upstream
- COUNT(DISTINCT col) → APPROX_COUNT_DISTINCT(col): flag as approximate alternative
- Missing partition filter on a date/timestamp-partitioned column: name the column if detectable
- CROSS JOIN without a WHERE filter bridging both sides: flag as potential Cartesian product
- Full table reference with no WHERE predicate: flag for partition/cluster review
- Window frame ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW on very wide tables: flag

### Output format (use exactly this structure — no deviations)

---
### BigQuery Performance Report: `<filename.sql>`
**Source**: `<gs://bucket/path/file.sql>`

#### Issues Found
| # | Severity | Category | Description | Location |
|---|---|---|---|---|
| 1 | 🔴 HIGH | Anti-pattern | NOT IN subquery — full scan risk | Line 23 |
| 2 | 🟠 MEDIUM | Redundancy | Subquery `sub_x` repeated 3× | Lines 10, 34, 67 |
| 3 | 🟡 LOW | Dead code | ORDER BY inside CTE has no effect | Line 8 |
| 4 | 🔵 REVIEW | Flag only | SELECT * — explicit columns recommended | Line 1 |

Severity scale: 🔴 HIGH (scan/join explosion) · 🟠 MEDIUM (redundant work) · 🟡 LOW (minor) · 🔵 REVIEW (flag-only)

#### Optimized SQL
```sql
-- ================================================================
-- Optimized by MyAgent
-- Original : <gs://bucket/path/file.sql>
-- Changes  : <comma-separated list of change types applied>
-- Parity   : VERIFIED — produces identical rows, columns, and order
-- ================================================================
<complete rewritten SQL — never truncate or summarize>
```

#### Change Log
| # | Change Type | Original Pattern | Optimized Pattern | Parity |
|---|---|---|---|---|
| 1 | Rewrite | NOT IN (SELECT id FROM …) | LEFT JOIN … WHERE id IS NULL | ✓ Identical |
| 2 | Extract CTE | Inline subquery (×3) | WITH cte_name AS (…) | ✓ Identical |
| 3 | Remove | ORDER BY inside CTE | — removed — | ✓ Identical |

#### Performance Impact Report
Populated from estimate_sql_cost() called on both original and optimized SQL.
If estimate_sql_cost() returned an error for either version, state the error and omit that row.

| Metric | Original | Optimized | Delta |
|---|---|---|---|
| Est. bytes scanned | X.XX TB | X.XX TB | −X.XX TB (−X%) |
| Est. query cost (@ $6.25/TB) | $X.XX | $X.XX | −$X.XX (−X%) |
| Line count | N | N | −N (−X%) |
| CTE count | 0 | N | +N |
| Subquery count | N | N | −N |
| JOIN count | N | N | ±N |
| Max nesting depth | N | N | −N |
| Anti-patterns fixed | — | — | N |
| Flag-only issues | — | — | N |

**Cost basis**: BigQuery on-demand pricing at $6.25 / TB scanned.
**Note**: Dry-run estimates are deterministic for queries with partition filters; without a filter
they represent a worst-case upper bound (full table scan). Actual slot time and cache hits may
reduce the effective cost further.

#### Functional Parity Statement
State explicitly which guarantees hold, e.g.:
"The optimized SQL produces identical rows, column names, data types, and row ordering
to the original under all valid input conditions. No semantic changes were made."
List any edge case where behavior could theoretically differ (e.g., NULL handling in NOT IN vs LEFT JOIN when the subquery contains NULLs — document the assumption made).

#### Further Opportunities (manual review required)
- 🔵 `SELECT *` at line 1: replace with explicit column list after confirming schema — reduces bytes scanned
- 🔵 `UNION` at line 45: replace with `UNION ALL` if upstream rows are guaranteed distinct — avoids dedup sort
---"""

_GENERAL_RULES = """
## General rules
- For query results: write one sentence (e.g. "Found 42 rows.") — NEVER reproduce the rows or a markdown table in your text. The UI renders the full table automatically.
- For schema: show verbatim in a ```json block; do not reformat.
- For JSON output requests: return a ```json array of objects.
- Always include citations for every piece of data returned."""

SYSTEM_PROMPT = "\n".join(filter(None, [
    "You are a data and pipeline assistant with access to the sources listed below.",
    _BQ_SECTION,
    _EXCEL_SECTION,
    _MAPPING_SECTION,
    _SCHEMA_AUDIT_SECTION,
    _GCS_SECTION,
    _AUDIT_SECTION,
    _TUNING_SECTION,
    _GENERAL_RULES,
]))


# ---------------------------------------------------------------------------
# BigQuery tools
# ---------------------------------------------------------------------------

def list_datasets(project: str) -> dict[str, Any]:
    """List all datasets in a project. Args: project: GCP project ID."""
    return bq_service.list_datasets(project)


def list_tables(project: str, dataset: str) -> dict[str, Any]:
    """List all tables in a dataset. Args: project: GCP project ID. dataset: Dataset ID."""
    return bq_service.list_tables(project, dataset)


def get_table_schema(project: str, dataset: str, table: str) -> dict[str, Any]:
    """Get schema and row count. Args: project: GCP project ID. dataset: Dataset ID. table: Table ID."""
    return bq_service.get_table_schema(project, dataset, table)


def execute_sql(sql_query: str) -> dict[str, Any]:
    """Run a BigQuery SELECT. Args: sql_query: SQL using fully-qualified `project.dataset.table` names."""
    return bq_service.execute_sql(re.sub(r"```(?:sql)?\s*", "", sql_query).strip().strip("`"))


def estimate_sql_cost(sql_text: str) -> dict[str, Any]:
    """
    Dry-run sql_text against BigQuery (no data read) and return:
    bytes_processed, bytes_processed_human, estimated_cost_usd, and structural stats
    (line_count, cte_count, subquery_count, join_count, max_nesting_depth).
    Call this on both the original and optimized SQL to populate the Performance Impact Report.
    Args: sql_text: full SQL string to analyse (not a file path).
    """
    sql = re.sub(r"```(?:sql)?\s*", "", sql_text).strip().strip("`")
    return bq_service.estimate_sql_cost(sql)


# ---------------------------------------------------------------------------
# Excel (DuckDB) tools — registered only when EXCEL_DATA_PATH is set
# ---------------------------------------------------------------------------

if settings.EXCEL_DATA_PATH:
    import excel_service

    def list_excel_files() -> dict[str, Any]:
        """List all Excel files loaded from the data path with their sheet names."""
        return excel_service.list_excel_files()

    def list_excel_tables() -> dict[str, Any]:
        """List all queryable Excel tables with column names and citations. Call before querying."""
        return excel_service.list_excel_tables()

    def get_excel_schema(table_name: str) -> dict[str, Any]:
        """Get columns and row count for an Excel table. Args: table_name: from list_excel_tables."""
        return excel_service.get_excel_schema(table_name)

    def query_excel(sql_query: str) -> dict[str, Any]:
        """Run DuckDB SQL against Excel tables. Args: sql_query: SQL using table names from list_excel_tables."""
        sql = re.sub(r"```(?:sql)?\s*", "", sql_query).strip().strip("`")
        return excel_service.query_excel(sql)

    _EXCEL_TOOLS: list = [list_excel_files, list_excel_tables, get_excel_schema, query_excel]
else:
    _EXCEL_TOOLS = []


# ---------------------------------------------------------------------------
# Mapping spec tools — registered only when EXCEL_DATA_PATH is set
# ---------------------------------------------------------------------------

if settings.EXCEL_DATA_PATH:
    import mapping_service

    def list_mapping_tables() -> dict[str, Any]:
        """List all tables that have an Excel Master or Mapping spec file indexed."""
        return mapping_service.list_mapping_tables()

    def get_table_mapping(table_name: str) -> dict[str, Any]:
        """Get the full Excel spec for a table: source/target pairs, transformation rules, citations. Args: table_name: table to look up."""
        return mapping_service.get_table_mapping(table_name)

    _MAPPING_TOOLS: list = [list_mapping_tables, get_table_mapping]
else:
    _MAPPING_TOOLS = []

if settings.SCHEMA_METADATA_PROJECT:
    import mapping_service as _mapping_service_audit

    def run_schema_audit() -> dict[str, Any]:
        """Reconcile MySQL source columns against live BigQuery view schemas and write Excel + DDL JSON reports. No arguments needed."""
        return _mapping_service_audit.run_schema_audit()

    _SCHEMA_AUDIT_TOOLS: list = [run_schema_audit]
else:
    _SCHEMA_AUDIT_TOOLS = []


# ---------------------------------------------------------------------------
# GCS tools — read_gcs_path is always available; SQL-specific tools require SQL_GCS_BUCKET
# ---------------------------------------------------------------------------

def read_gcs_path(gcs_uri: str) -> dict[str, Any]:
    """Read any GCS file by its full URI. Args: gcs_uri: full gs://bucket/path URI (e.g. gs://my-bucket/sql/orders.sql)."""
    return gcs_service.read_gcs_path(gcs_uri)


if settings.SQL_GCS_BUCKET:
    import gcs_service

    def list_sql_files(prefix: str = "") -> dict[str, Any]:
        """List .sql files in the SQL GCS bucket. Args: prefix: optional path prefix to narrow results."""
        return gcs_service.list_sql_files(prefix)

    def find_sql_for_table(table_name: str) -> dict[str, Any]:
        """Find and read the SQL transformation file for a table from GCS. Args: table_name: table to locate."""
        return gcs_service.find_sql_for_table(table_name)

    def read_sql_file(path: str) -> dict[str, Any]:
        """Read a specific SQL file from the SQL GCS bucket. Args: path: blob path from list_sql_files."""
        return gcs_service.read_sql_file(path)

    _GCS_TOOLS: list = [list_sql_files, find_sql_for_table, read_sql_file]
else:
    _GCS_TOOLS = []


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------

TOOLS = (
    [list_datasets, list_tables, get_table_schema, execute_sql, estimate_sql_cost]
    + _EXCEL_TOOLS
    + _MAPPING_TOOLS
    + _SCHEMA_AUDIT_TOOLS
    + [read_gcs_path]
    + _GCS_TOOLS
)
TOOL_MAP = {fn.__name__: fn for fn in TOOLS}


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------

class MyAgent:
    def __init__(self) -> None:
        self.genai_client = None
        self.history: list = []

    def get_genai_client(self):
        if self.genai_client is None:
            from google import genai
            self.genai_client = genai.Client(
                vertexai=True,
                project=settings.GCP_PROJECT_ID,
                location=settings.VERTEX_LOCATION,
            ) if settings.GCP_PROJECT_ID else genai.Client()
        return self.genai_client

    def ask(self, prompt: str) -> dict[str, Any]:
        self.history.append(types.Content(role="user", parts=[types.Part(text=prompt)]))
        result: dict[str, Any] = {"text": "", "tables": []}

        for _ in range(15):  # extra headroom for multi-step audit workflows
            response = self.get_genai_client().models.generate_content(
                model=settings.GEMINI_MODEL,
                contents=self.history,
                config=types.GenerateContentConfig(
                    tools=TOOLS,
                    system_instruction=SYSTEM_PROMPT,
                    automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
                ),
            )

            model_content = response.candidates[0].content
            self.history.append(model_content)

            fn_parts = [p for p in (model_content.parts or []) if p.function_call is not None]
            if not fn_parts:
                text = response.text or ""
                # Strip markdown table lines — the UI renders dataframes; text must not duplicate them
                if result["tables"]:
                    lines = [l for l in text.split("\n") if not l.lstrip().startswith("|")]
                    text = re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()
                result["text"] = text
                break

            response_parts = []
            for part in fn_parts:
                fn_name = part.function_call.name
                raw_args = part.function_call.args
                fn_args = MessageToDict(raw_args) if hasattr(raw_args, "DESCRIPTOR") else dict(raw_args or {})
                log.debug("Tool: %s(%s)", fn_name, fn_args)

                try:
                    fn_result = TOOL_MAP[fn_name](**fn_args)
                except Exception as exc:
                    fn_result = {"error": str(exc)}

                # Surface tabular results to the UI
                if "columns" in fn_result:
                    result["tables"].append({
                        "columns": fn_result["columns"],
                        "rows": fn_result["rows"],
                    })

                # Trim large row payloads sent back to the LLM
                llm_result = fn_result.copy()
                if isinstance(llm_result.get("rows"), list) and len(llm_result["rows"]) > 10:
                    llm_result["rows"] = llm_result["rows"][:10]
                    llm_result["truncated_note"] = "Data truncated for context. UI has full table."

                # Trim large file content — 32 000 chars keeps even complex SQL files whole
                # while preventing runaway context growth from very large DAG/log files
                if isinstance(llm_result.get("content"), str) and len(llm_result["content"]) > 32_000:
                    llm_result["content"] = llm_result["content"][:32_000]
                    llm_result["content_truncated"] = "File truncated at 32 000 chars for context."

                response_parts.append(
                    types.Part.from_function_response(name=fn_name, response=llm_result)
                )

            self.history.append(types.Content(role="tool", parts=response_parts))

        return result

    def reset(self) -> None:
        self.history = []
        self.genai_client = None
