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
- show_excel_file(file_name, sheet_name?): show the full contents of an Excel file by filename — use this whenever the user asks to "show", "display", or "open" a file. No need to know the internal table name.
- list_excel_files(): list all loaded Excel files and their sheets
- list_excel_tables(): list all queryable tables with column names (call before querying)
- get_excel_schema(table_name): get columns and row count for one table
- query_excel(sql_query): DuckDB SQL SELECT against Excel tables; cross-file JOINs supported

Rules:
- For "show / display / open file" requests always use show_excel_file(file_name) — never guess a table name.
- Always call list_excel_tables() before query_excel if table names are unknown.
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

_COMPOSER_SECTION = (f"""
## Airflow Composer V3  (environment: {settings.COMPOSER_ENVIRONMENT or "not configured"})

### GCS-based tools (work with COMPOSER_DAG_BUCKET only)
- list_composer_environments(location?): list ALL Composer environments in the project; call this when the user asks to "list composers" or "show all environments". location defaults to COMPOSER_LOCATION; pass "-" for all regions.
- get_composer_environment(): verify connectivity; returns state, Airflow URI, and GCS DAG bucket
- list_dag_files(): list all DAG Python (.py) files in the Composer GCS DAGs folder
- find_dag_for_table(table_name): full-text search DAG files for table references
- read_dag_file(dag_file_path): read the raw Python source of a DAG file from GCS

### Airflow REST API tools (require COMPOSER_ENVIRONMENT; return live/rendered data)
- list_dags_for_environment(environment_name): list all DAGs for a NAMED Composer environment — use this whenever the user specifies an environment by name (e.g. "list DAGs for composer xyz"). Resolves the Airflow URI automatically.
- list_airflow_dags(): list all DAGs for the DEFAULT configured environment
- get_dag_source(dag_id): **primary DAG code tool** — resolves the dag_id to its GCS file path
  via the Airflow API and returns the full Python source; use this for DAG optimization
- list_dag_tasks(dag_id): list every task in a DAG with its operator class, template fields,
  and a `likely_has_sql` flag
- get_dag_runs(dag_id): return the most recent DAG runs with state and execution dates
- find_sql_tasks_in_dag(dag_id): shortcut — filters list_dag_tasks to SQL-bearing tasks only
- get_rendered_task_sql(dag_id, task_id, dag_run_id): calls the Airflow renderedFields endpoint
  and returns the Jinja-compiled SQL exactly as Airflow sent it to BigQuery

### Rules
- Cite GCS DAG files as: [DAG: dag_id | gs://bucket/dags/file.py]
- Cite extracted task SQL as: [DAG: dag_id | Task: task_id | Run: dag_run_id]
- For get_rendered_task_sql: prefer a dag_run_id whose state is 'success'""" if settings.COMPOSER_ENVIRONMENT or settings.COMPOSER_DAG_BUCKET else "")

_AUDIT_SECTION = """
## Transformation Audit Workflow
When the user asks to audit, trace, validate, or compare a table:

Step 1 — Gather specs:     call get_table_mapping(table_name)
Step 2 — Locate SQL:       call find_sql_for_table(table_name)
Step 3 — Locate DAG:       call find_dag_for_table(table_name)
Step 4 — Analyse & report using this exact structure:

---
### Audit Report: <TableName>

**Traceability**
| Layer | Reference |
|---|---|
| Excel Spec | [file.xlsx — mapping spec] |
| SQL File   | [gs://bucket/path/file.sql] |
| DAG        | [DAG: dag_id \\| gs://bucket/dags/file.py] |
| DAG Tasks  | task_a, task_b |

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
- DAG:   [DAG: dag_id]
---""" if settings.EXCEL_DATA_PATH or settings.SQL_GCS_BUCKET or settings.COMPOSER_ENVIRONMENT else ""

_DAG_SQL_AUDIT_SECTION = """
## DAG Task SQL Extraction & Performance Audit
Triggered when the user asks to extract, audit, tune, or optimize SQL from an Airflow DAG task.

### Extraction workflow (always follow these steps in order)

Step 1 — Discover SQL tasks
  Call find_sql_tasks_in_dag(dag_id)
  → Identifies which tasks carry SQL based on operator class and template fields.
  → If the user did not specify a task_id, present the sql_tasks list and ask which to audit,
    or audit all of them sequentially.

Step 2 — Get a recent successful run
  Call get_dag_runs(dag_id)
  → Pick the most recent run with state = "success".
  → If no successful run exists, use the most recent run of any state and note the caveat.

Step 3 — Extract compiled SQL
  Call get_rendered_task_sql(dag_id, task_id, dag_run_id)
  → Use primary_sql as the SQL to audit.
  → If multiple sql_fields are returned, note all of them but audit primary_sql first.

Step 4 — Performance audit
  Apply the full BigQuery SQL Performance Tuning workflow to the extracted primary_sql.
  Use the citation format: [DAG: dag_id | Task: task_id | Run: dag_run_id]
  instead of a GCS URI in the report header.

Step 5 — If the user requests a GCS comparison
  Call find_sql_for_table(table_name) or read_sql_file(path) to retrieve the stored .sql file,
  then compare it to the extracted rendered SQL to detect drift between the spec file and
  the actual SQL that Airflow is running.

### Critical rules
- NEVER run or re-execute any SQL — extraction only, read-only.
- The rendered SQL is the ground truth for what Airflow executed; the GCS .sql file may differ
  due to Jinja variables or manual edits — flag any discrepancy explicitly.
- Functional parity of the optimized SQL must be verified against the rendered (extracted) SQL,
  not the GCS file, since Jinja rendering may have altered the logic.""" if settings.COMPOSER_ENVIRONMENT else ""

_DAG_OPTIMIZATION_SECTION = """
## Composer V3 DAG Code Optimization
Triggered when the user asks to optimize, refactor, or improve an Airflow DAG.

### Retrieval workflow
1. If the user provides a dag_id → call get_dag_source(dag_id) to fetch the Python source.
2. If only a file path is known → call read_dag_file(path) instead.
3. Analyze the full source against every pattern below.
4. Produce the structured report defined at the end of this section.

---
### Pattern reference

#### 🔴 HIGH — Deprecated operators / breaking changes
These will fail or produce incorrect results in Composer V3 (Airflow 2.x+):

| Deprecated | Replacement | Import path |
|---|---|---|
| `BigQueryOperator` | `BigQueryInsertJobOperator` | `airflow.providers.google.cloud.operators.bigquery` |
| `BigQueryExecuteQueryOperator` | `BigQueryInsertJobOperator` | same |
| `SubDagOperator` | `TaskGroup` | `airflow.utils.task_group` |
| `DummyOperator` | `EmptyOperator` | `airflow.operators.empty` |
| `execution_date` Jinja var | `{{ logical_date }}` or `{{ ds }}` | n/a |
| `provide_context=True` in PythonOperator | Remove — context injected automatically in 2.x | n/a |
| `schedule_interval=` kwarg | `schedule=` | n/a |
| `python_callable` accessing `**kwargs` for context | `get_current_context()` inside the callable | `airflow.operators.python` |

#### 🔴 HIGH — Slot-blocking sensor patterns
Blocking sensors hold a worker slot for the entire wait period, starving other tasks:
- `GCSObjectExistenceSensor` without `deferrable=True` → use `GCSObjectExistenceAsyncSensor`
  or set `deferrable=True` (requires Airflow 2.2+ and a triggerer component, available in Composer V3)
- `BigQuerySensor` / `BigQueryCheckSensor` → switch to deferrable variant
- `TimeDeltaSensor` → `TimeDeltaSensorAsync`
- Any sensor with `poke_interval < 60` and long expected wait → increase interval or go deferrable

#### 🟠 MEDIUM — Deferrable operator upgrades (reduce slot consumption)
These upgrades are safe and reduce worker slot usage for long-running BQ jobs:
- `BigQueryInsertJobOperator(deferrable=False)` → add `deferrable=True`
- `DataflowCreateJavaJobOperator` → add `deferrable=True`
- `DataprocSubmitJobOperator` → add `deferrable=True`

#### 🟠 MEDIUM — Missed parallelism
- Sequential tasks with no data dependency → connect to the same upstream and let them run in parallel
- Python `for` loop creating chained tasks (`t1 >> t2 >> t3`) where each iteration is independent
  → use dynamic task mapping: `task.expand(arg=list_of_values)`
- Repeated identical sensor tasks guarding the same condition → single sensor with multiple downstream

#### 🟠 MEDIUM — XCom misuse
- XCom storing DataFrames, large strings, or file contents → store data in GCS; XCom only the URI
- `xcom_push` / `xcom_pull` crossing TaskGroup boundaries for large payloads → same GCS pattern
- `do_xcom_push=True` on operators whose output is not consumed → set `do_xcom_push=False`

#### 🟠 MEDIUM — DAG parse-time side effects (slows scheduler)
- `Variable.get()` at module level → move inside the callable or use `{{ var.value.key }}` in templates
- Database or GCS queries executed at import time → defer to task execution
- Heavy `import` statements at module top level that are only needed by one task
  → move the import inside the function

#### 🟡 LOW — Missing best practices
- No `catchup=False` when the DAG is not designed for backfill → add explicitly
- No `max_active_runs` set → add `max_active_runs=1` for stateful pipelines
- No `tags` on the DAG → add descriptive tags for UI filtering
- No `doc_md` → add a markdown description
- No `retries` / `retry_delay` in `default_args` → add sensible defaults
- `start_date` uses `datetime.now()` → use a fixed date; dynamic start_date causes re-scheduling bugs
- `start_date` passed as `datetime(2024,1,1)` without `timezone.utc` → add UTC timezone
- Cron expression for simple schedules → use `@daily`, `@hourly`, `@weekly` shortcuts

#### 🔵 REVIEW — Require understanding of business logic
- Changing `trigger_rule` on any task
- Adding or removing `depends_on_past=True`
- Converting to Dataset-based scheduling (Airflow 2.4+) — changes what triggers the DAG
- Splitting or merging TaskGroups
- Removing `on_failure_callback` / `on_retry_callback`

---
### Composer V3 — correct import paths (always verify these are used)
```python
# BigQuery
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
    BigQueryValueCheckOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteTableOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceAsyncSensor

# Structure
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, get_current_context

# Scheduling helpers
from airflow.utils import timezone
```

---
### Output format (use exactly this structure)

---
### DAG Optimization Report: `<dag_file.py>`
**Source**: `[DAG: dag_id | gs://bucket/dags/dag_file.py]`
**Airflow version target**: Composer V3 / Airflow 2.x

#### Issues Found
| # | Severity | Category | Description | Location |
|---|---|---|---|---|
| 1 | 🔴 HIGH | Deprecated operator | `BigQueryOperator` removed in Airflow 2.x | Line 12 |
| 2 | 🔴 HIGH | Slot-blocking sensor | `GCSObjectExistenceSensor` without deferrable | Line 38 |
| 3 | 🟠 MEDIUM | XCom misuse | DataFrame pushed via XCom (large payload) | Line 67 |
| 4 | 🟡 LOW | Missing config | `catchup` not set; defaults to True | Line 5 |
| 5 | 🔵 REVIEW | Parallelism | Tasks B, C, D are sequential but independent | Lines 80-90 |

#### Optimized DAG
```python
# ================================================================
# Optimized by MyAgent for Composer V3 / Airflow 2.x
# Original : gs://bucket/dags/dag_file.py
# Changes  : <comma-separated list of change types>
# Parity   : VERIFIED — identical task graph, data flow, and schedule
# ================================================================
<complete rewritten Python DAG — never truncate or omit sections>
```

#### Change Log
| # | Change | Original | Optimized | Parity |
|---|---|---|---|---|
| 1 | Replace operator | `BigQueryOperator(sql=…)` | `BigQueryInsertJobOperator(configuration={…})` | ✓ Identical execution |
| 2 | Deferrable sensor | `GCSObjectExistenceSensor(…)` | `GCSObjectExistenceAsyncSensor(…)` | ✓ Same trigger condition |
| 3 | Fix parse-time call | `Variable.get('x')` at module level | `{{ var.value.x }}` in template | ✓ Same value |

#### Functional Parity Statement
Confirm: "The optimized DAG produces an identical task execution graph, respects the same
dependencies, uses the same connections, and processes the same data as the original."
List any assumption about business logic (e.g., "assumed tasks B, C, D are stateless and
can safely be parallelized — verify upstream data isolation before deploying").

#### Further Opportunities (manual review required)
- 🔵 Dynamic task mapping: tasks `process_jan`, `process_feb`, … follow identical pattern — consider `.expand()`
- 🔵 Dataset scheduling: DAG triggered by GCS sensor could use Dataset-based scheduling (Airflow 2.4+)
---""" if settings.COMPOSER_ENVIRONMENT or settings.COMPOSER_DAG_BUCKET else ""

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

_GIT_SECTION = (f"""
## Git Repository  (branch: {settings.GIT_BRANCH or "default"})

### Navigation tools
- get_git_status(): verify connectivity; returns branch, latest commit, remote URL, and configured paths
- list_git_files(path_prefix?): list files under a subdirectory (e.g. "{settings.GIT_DAG_PATH}" or "{settings.GIT_SQL_PATH}")
- read_git_file(file_path): read a file by its repo-relative path (e.g. "{settings.GIT_DAG_PATH}my_dag.py")

### Search tools
- find_dag_in_git(dag_name): fuzzy-search the DAG directory for a Python file matching dag_name
- find_sql_in_git(table_name): fuzzy-search the SQL directory for a .sql file matching table_name

### Comparison tool
- compare_files(content_a, content_b, label_a, label_b): produce a unified diff between any two text contents.
  Typical label patterns:
    label_a = "git:branch/path"   label_b = "gcs:gs://bucket/path"
    label_a = "git:branch/path"   label_b = "airflow:dag_id/task_id/run_id"
    label_a = "original"          label_b = "optimised"

### Commit tool
- commit_file_to_git(file_path, content, commit_message): write content to the repo, commit, and push.
  Use this only when the user explicitly requests a commit or asks to "save changes to git".
  file_path is relative to the repo root (e.g. "{settings.GIT_DAG_PATH}my_dag.py").

### Rules
- Always call get_git_status() first to confirm the repo is accessible.
- Cite git files as: [Git: {settings.GIT_BRANCH}/path]
- Never commit without explicit user instruction ("commit", "save to git", "push the changes").
- When comparing, always surface the diff in a ```diff code block.""" if settings.GIT_REPO_URL or settings.GIT_LOCAL_PATH else "")

_GIT_COMPARISON_SECTION = """
## Git ↔ GCS / Airflow Drift Detection
Triggered when the user asks to compare, diff, or check drift between git and GCS (or Airflow).

### Workflow

#### Git vs GCS SQL file
Step 1 — Read git version:     call find_sql_in_git(table_name)
Step 2 — Read GCS version:     call find_sql_for_table(table_name) or read_gcs_path(uri)
Step 3 — Diff:                 call compare_files(git_content, gcs_content,
                                  label_a="git:branch/path", label_b="gcs:gs://…")
Step 4 — Report: produce the structured Drift Report below.

#### Git vs Airflow rendered SQL
Step 1 — Read git version:     call find_sql_in_git(table_name) or find_dag_in_git(dag_id)
Step 2 — Extract live SQL:     call find_sql_tasks_in_dag → get_dag_runs → get_rendered_task_sql
Step 3 — Diff:                 call compare_files(git_content, rendered_sql,
                                  label_a="git:branch/path", label_b="airflow:dag/task/run")
Step 4 — Report: produce the structured Drift Report below.

#### Git vs Composer GCS DAG file
Step 1 — Read git version:     call find_dag_in_git(dag_id)
Step 2 — Read GCS version:     call get_dag_source(dag_id) or read_dag_file(path)
Step 3 — Diff:                 call compare_files(git_content, gcs_content,
                                  label_a="git:branch/path", label_b="gcs:gs://…")
Step 4 — Report: produce the structured Drift Report below.

### Drift Report format

---
### Drift Report: `<file_name>`
**Git source**:  `[Git: branch/path]`
**Remote source**: `[gs://…]` or `[DAG: id | Task: id | Run: id]`

#### Summary
| Metric | Value |
|---|---|
| Identical | Yes / No |
| Lines in git | N |
| Lines in remote | N |
| Lines added (remote has, git lacks) | N |
| Lines removed (git has, remote lacks) | N |

#### Diff
```diff
<unified diff output>
```

#### Analysis
<Numbered list of meaningful differences — renamed columns, added/removed CTEs, logic changes,
Jinja template drift, operator changes, etc.  If identical, state "No differences found.">

#### Recommendation
- If identical: "Git and remote are in sync — no action required."
- If diverged: list the specific changes needed to reconcile, and whether
  `commit_file_to_git()` or a manual GCS upload is the right next step.
---""" if (settings.GIT_REPO_URL or settings.GIT_LOCAL_PATH) and (settings.SQL_GCS_BUCKET or settings.COMPOSER_ENVIRONMENT or settings.COMPOSER_DAG_BUCKET) else ""

_GIT_WRITEBACK_SECTION = """
## Git Write-Back after Optimisation
When the user asks to commit, save, or push an optimised DAG or SQL back to git:

Step 1 — Confirm the file path
  Use the path from find_dag_in_git() / find_sql_in_git(), or ask the user.

Step 2 — Confirm content
  The content to commit is the `optimised_sql` / optimised DAG from the tuning report.
  Never commit untested or truncated content.

Step 3 — Commit
  Call commit_file_to_git(file_path, content, commit_message) where commit_message follows
  the convention: "perf: optimise <file> — <one-line summary of changes>"

Step 4 — Report
  Show the returned commit_sha, branch, and push_status.
  If push_status contains "push_failed", tell the user the commit exists locally and
  provide the push hint from the response.""" if settings.GIT_REPO_URL or settings.GIT_LOCAL_PATH else ""

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
    _COMPOSER_SECTION,
    _AUDIT_SECTION,
    _DAG_SQL_AUDIT_SECTION,
    _DAG_OPTIMIZATION_SECTION,
    _TUNING_SECTION,
    _GIT_SECTION,
    _GIT_COMPARISON_SECTION,
    _GIT_WRITEBACK_SECTION,
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

    def show_excel_file(file_name: str, sheet_name: str = "") -> dict[str, Any]:
        """Show the full contents of an Excel file by its filename. Args: file_name: filename or partial match (e.g. 'AAA.xlsx'). sheet_name: optional sheet name to narrow results."""
        return excel_service.show_excel_file(file_name, sheet_name)

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

    _EXCEL_TOOLS: list = [show_excel_file, list_excel_files, list_excel_tables, get_excel_schema, query_excel]
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
# Composer / DAG tools — registered when Composer environment is configured
# ---------------------------------------------------------------------------

if settings.COMPOSER_ENVIRONMENT or settings.COMPOSER_DAG_BUCKET:
    import composer_service

    # ── GCS-based tools (available with either COMPOSER_ENVIRONMENT or COMPOSER_DAG_BUCKET) ──

    def list_composer_environments(location: str = "") -> dict[str, Any]:
        """List all Composer environments in the configured project. Args: location: GCP region (default: COMPOSER_LOCATION). Pass '-' for all regions."""
        return composer_service.list_composer_environments(location)

    def get_composer_environment() -> dict[str, Any]:
        """Get metadata for the configured Composer V3 environment (state, GCS DAG bucket, Airflow URI)."""
        return composer_service.get_composer_environment()

    def list_dag_files() -> dict[str, Any]:
        """List all DAG Python files in the Composer environment's GCS DAGs folder."""
        return composer_service.list_dag_files()

    def find_dag_for_table(table_name: str) -> dict[str, Any]:
        """Search all Composer DAG files for references to a table. Returns DAG IDs, task IDs, and context. Args: table_name: table to search for."""
        return composer_service.find_dag_for_table(table_name)

    def read_dag_file(dag_file_path: str) -> dict[str, Any]:
        """Read a Composer DAG file from GCS. Args: dag_file_path: blob path from list_dag_files."""
        return composer_service.read_dag_file(dag_file_path)

    def list_dags_for_environment(environment_name: str) -> dict[str, Any]:
        """List all DAGs for a named Composer environment. Resolves the Airflow URI automatically. Args: environment_name: Composer environment name, e.g. 'xyz'."""
        return composer_service.list_dags_for_environment(environment_name)

    _COMPOSER_TOOLS: list = [
        list_composer_environments, get_composer_environment,
        list_dag_files, find_dag_for_table, read_dag_file,
        list_dags_for_environment,
    ]

    # ── Airflow REST API tools (require COMPOSER_ENVIRONMENT to resolve the Airflow URI) ──

    if settings.COMPOSER_ENVIRONMENT:
        import airflow_service

        def list_airflow_dags() -> dict[str, Any]:
            """List all DAGs in the default configured Airflow environment with pause state and tags."""
            return airflow_service.list_airflow_dags()

        def get_dag_source(dag_id: str) -> dict[str, Any]:
            """Fetch the full Python source of a DAG by its dag_id. Resolves the GCS file path automatically via the Airflow API. Use this as the first step for DAG optimization. Args: dag_id: the DAG identifier."""
            return airflow_service.get_dag_source(dag_id)

        def list_dag_tasks(dag_id: str) -> dict[str, Any]:
            """List all tasks in a DAG with operator class, template fields, and likely_has_sql flag. Args: dag_id: the DAG identifier."""
            return airflow_service.list_dag_tasks(dag_id)

        def get_dag_runs(dag_id: str) -> dict[str, Any]:
            """Get the most recent DAG runs with state and execution dates. Provides dag_run_id needed by get_rendered_task_sql. Args: dag_id: the DAG identifier."""
            return airflow_service.get_dag_runs(dag_id)

        def find_sql_tasks_in_dag(dag_id: str) -> dict[str, Any]:
            """Find all tasks in a DAG that likely contain SQL, based on operator class and template fields. First step of the DAG SQL extraction workflow. Args: dag_id: the DAG identifier."""
            return airflow_service.find_sql_tasks_in_dag(dag_id)

        def get_rendered_task_sql(dag_id: str, task_id: str, dag_run_id: str) -> dict[str, Any]:
            """Extract the compiled/rendered SQL from a specific Airflow task instance. Returns the Jinja-resolved SQL exactly as Airflow sent it to BigQuery. Args: dag_id: DAG identifier. task_id: task identifier. dag_run_id: run ID from get_dag_runs (prefer state=success)."""
            return airflow_service.get_rendered_task_sql(dag_id, task_id, dag_run_id)

        _COMPOSER_TOOLS += [
            list_airflow_dags, get_dag_source,
            list_dag_tasks, get_dag_runs,
            find_sql_tasks_in_dag, get_rendered_task_sql,
        ]

else:
    _COMPOSER_TOOLS = []


# ---------------------------------------------------------------------------
# Git tools — registered when GIT_REPO_URL or GIT_LOCAL_PATH is set
# ---------------------------------------------------------------------------

if settings.GIT_REPO_URL or settings.GIT_LOCAL_PATH:
    import git_service

    def get_git_status() -> dict[str, Any]:
        """Verify git connectivity. Returns branch, latest commit, remote URL, and DAG/SQL paths. Call first before any other git tool."""
        return git_service.get_git_status()

    def list_git_files(path_prefix: str = "") -> dict[str, Any]:
        """List files in the git repo under a subdirectory. Args: path_prefix: relative path within the repo (e.g. 'dags/' or 'sql/'). Leave empty for repo root."""
        return git_service.list_git_files(path_prefix)

    def read_git_file(file_path: str) -> dict[str, Any]:
        """Read a file from the git repo by its repo-relative path. Args: file_path: path relative to repo root (e.g. 'dags/my_dag.py')."""
        return git_service.read_git_file(file_path)

    def find_dag_in_git(dag_name: str) -> dict[str, Any]:
        """Fuzzy-search the git DAG directory for a Python file matching dag_name. Returns the best-matching file with its full content. Args: dag_name: DAG id, partial file name, or table name."""
        return git_service.find_dag_in_git(dag_name)

    def find_sql_in_git(table_name: str) -> dict[str, Any]:
        """Fuzzy-search the git SQL directory for a .sql file matching table_name. Returns the best-matching file with full content. Args: table_name: table name, partial file name, or DAG-style identifier."""
        return git_service.find_sql_in_git(table_name)

    def compare_files(
        content_a: str,
        content_b: str,
        label_a: str = "version_a",
        label_b: str = "version_b",
    ) -> dict[str, Any]:
        """Compare two text contents and return a unified diff. Args: content_a: reference version (e.g. git source). content_b: candidate version (e.g. GCS or rendered Airflow SQL). label_a: label for content_a. label_b: label for content_b."""
        return git_service.compare_files(content_a, content_b, label_a, label_b)

    def commit_file_to_git(
        file_path: str,
        content: str,
        commit_message: str,
    ) -> dict[str, Any]:
        """Write content to the git repo, commit, and push. Only call when the user explicitly requests a commit. Args: file_path: repo-relative path (e.g. 'dags/my_dag.py'). content: new file content. commit_message: descriptive commit message."""
        return git_service.commit_file_to_git(file_path, content, commit_message)

    _GIT_TOOLS: list = [
        get_git_status, list_git_files, read_git_file,
        find_dag_in_git, find_sql_in_git,
        compare_files, commit_file_to_git,
    ]
else:
    _GIT_TOOLS = []


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
    + _COMPOSER_TOOLS
    + _GIT_TOOLS
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

            self.history.append(types.Content(role="user", parts=response_parts))

        return result

    def reset(self) -> None:
        self.history = []
        self.genai_client = None
