## OSR Data Intelligence — Run Book

Ask questions in plain English about your BigQuery, Airflow, GCS, Excel, and Git data.

---

### BigQuery

| Example | What it does |
|---|---|
| `List all datasets` | Browses available datasets |
| `Show tables in my_dataset` | Lists tables in a dataset |
| `What columns does my_table have?` | Returns schema + row count |
| `How many orders were placed last month?` | Runs a SELECT query |
| `Estimate the cost of: SELECT * FROM …` | Dry-run cost estimate |

**Rules:** fully-qualified `project.dataset.table` names · SELECT only · no schema via full scan.

---

### Airflow / Cloud Composer

| Example | What it does |
|---|---|
| `List all DAGs` | Shows every DAG with pause state and tags |
| `Show tasks in my_dag` | Lists tasks with operator class |
| `Get recent runs for my_dag` | Returns run history with state |
| `Extract the SQL from my_dag / load_task` | Fetches Jinja-rendered SQL from the task instance |
| `Show the source code for my_dag` | Reads the DAG Python file from GCS |

> `COMPOSER_ENVIRONMENT` must be set in `.env` for Airflow REST API tools.

---

### GCS File Access

| Example | What it does |
|---|---|
| `Read gs://my-bucket/path/file.sql` | Reads any GCS file |
| `List SQL files` | Browses `.sql` files in the configured bucket |
| `Find SQL for table my_table` | Fuzzy-matches a `.sql` file by table name |

---

### Excel / Mapping Specs

| Example | What it does |
|---|---|
| `List all Excel tables` | Shows all loaded sheets |
| `Query the sales sheet where region = West` | DuckDB SELECT across Excel data |
| `Get the mapping spec for my_table` | Retrieves source→target transformation rules |

> Requires `EXCEL_DATA_PATH` in `.env`.

---

### Transformation Audit

```
Audit the transformation for my_table
```

Automatically: fetches Excel mapping spec → locates SQL file in GCS → finds the DAG → produces a structured report with mapping verification and discrepancies.

---

### Git Repository

| Example | What it does |
|---|---|
| `Show git status` | Verifies connectivity and branch |
| `List DAG files in git` | Lists `.py` files under `dags/` |
| `Read dags/my_pipeline.py from git` | Reads a file from the repo |

---

### Tips
- Include table names, DAG IDs, or project names when you know them.
- Ask follow-up questions — the agent remembers the conversation.
- Every answer cites its source (GCS URI, DAG ID, Excel file).