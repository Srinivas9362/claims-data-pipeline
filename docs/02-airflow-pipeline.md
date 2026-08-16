# Claims Data Pipeline — Airflow Pipeline Documentation

## 1. DAG Metadata

| Property | Value |
|----------|-------|
| **DAG ID** | `claims_pipeline` |
| **File** | `airflow/dags/claims_pipeline.py` |
| **Schedule** | `None` (manual trigger only) |
| **Catchup** | `False` |
| **Start date** | `2026-01-01` |
| **Tags** | `claims`, `aws`, `snowflake` |

---

## 2. DAG Configuration Constants

These are hardcoded at the top of the DAG file:

| Constant | Value |
|----------|-------|
| `AWS_CONN_ID` | `aws_claims` |
| `SNOWFLAKE_CONN_ID` | `snowflake_claims` |
| `PROJECT_NAME` | `CLAIMS_PIPELINE` |
| `PIPELINE_NAME` | `claims_pipeline` |
| `ENVIRONMENT` | `DEV` |
| `S3_BUCKET` | `claims-data-pipeline-2026-0808` |
| `S3_PREFIX` | `claims-data-pipeline/input/` |

---

## 3. Task Flow

```
generate_run_context
        │
        ▼
check_s3_file
        │
        ▼
load_raw
```

Dependency definition (line 1344):
```python
generate_run_context_task >> check_s3_file_task >> load_raw_task
```

---

## 4. Task 1 — `generate_run_context`

### Purpose
Build the pipeline run metadata used by all downstream tasks.

### Inputs
- `logical_date` — Airflow's execution date (used as default file date)
- `dag_run.conf` — optional user config, e.g. `{"file_date": "20260808"}`

### Output (XCom `return_value`)
A dict:
```python
{
    "run_id": "20260808101050",     # UTC timestamp, format %Y%m%d%H%M%S
    "file_date": "20260808",        # from logical_date or dag_run.conf
    "file_name": "claims_20260808.csv",
    "s3_bucket": "claims-data-pipeline-2026-0808",
    "s3_key": "claims-data-pipeline/input/claims_20260808.csv"
}
```

### Logic Details
1. `run_id` — generated from `timezone.utcnow()` in `%Y%m%d%H%M%S` format.
2. `file_date` — defaults to `logical_date.strftime("%Y%m%d")`. If `dag_run.conf["file_date"]` is provided, it overrides; the value is validated to be `YYYYMMDD`, otherwise a `ValueError` is raised.
3. `file_name` — `claims_{file_date}.csv`.
4. `s3_key` — `{S3_PREFIX}{file_name}`.

### Example `dag_run.conf`
```json
{
  "file_date": "20260808"
}
```

---

## 5. Task 2 — `check_s3_file`

### Purpose
Verify the expected input file exists in S3 before attempting to load.

### Inputs
- XCom `return_value` from `generate_run_context`

### Logic Details
1. Pulls run context from XCom. Raises `RuntimeError` if missing.
2. Instantiates `S3Hook(aws_conn_id="aws_claims")`.
3. Calls `hook.check_for_key(key, bucket_name)`.
4. If the key does not exist → raises `FileNotFoundError`.
5. On success, returns a status dict and logs a friendly message.

### Output (XCom `return_value`)
```python
{
    "status": "COMPLETED",
    "file_exists": True,
    "file_name": "claims_20260808.csv",
    "s3_key": "claims-data-pipeline/input/claims_20260808.csv",
    "s3_bucket": "claims-data-pipeline-2026-0808",
    "message": "S3 file found successfully: claims_20260808.csv"
}
```

---

## 6. Task 3 — `load_raw`

### Purpose
Execute a Snowflake `COPY INTO` to load the CSV file from the S3 stage into the RAW table.

### Inputs
- XCom `return_value` from `generate_run_context`

### The SQL
```sql
COPY INTO CLAIMS_DATA_DB.RAW.CLAIMS_RAW
FROM @CLAIMS_DATA_DB.RAW.CLAIMS_S3_STAGE/input/
FILE_FORMAT = (
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
)
PATTERN = '.*claims_20260808\.csv'
ON_ERROR = 'ABORT_STATEMENT';
```

> Note: The file name is escaped (`\.`) before being inserted into the PATTERN regex.

### Execution Details
1. Pulls `run_context` from XCom.
2. Builds `escaped_file_name` by replacing `.` with `\.`.
3. Instantiates `SnowflakeHook(snowflake_conn_id="snowflake_claims")`.
4. Executes the COPY and captures `cursor.sfqid` (Snowflake query ID).
5. **Immediately** pushes `snowflake_query_id` to XCom (key `snowflake_query_id`) so it survives even if subsequent logic fails.
6. Fetches the result row.

### Result Handling — 3 Cases

**Case 1 — 0 files processed**
```
result = [("0 files processed",)]
```
Returns `COMPLETED` with `load_status = "NO_FILES_PROCESSED"` (file already loaded / no new files).

**Case 2 — empty result**
```
result = []
```
Returns `COMPLETED` with `load_status = "NO_RESULT"`.

**Case 3 — normal copy result**
```
result = [[file_name, status, rows_parsed, rows_loaded, ...]]
```
- If `status == "LOADED"` → `task_status = "COMPLETED"`.
- Otherwise → `task_status = "FAILED"` and raises `RuntimeError`.

### Output (XCom keys)
- `snowflake_query_id` — the Snowflake query ID
- `load_raw_result` — full result dict with `status`, `load_status`, `rows_parsed`, `rows_loaded`, `errors_seen`, `first_error`, `snowflake_query_id`, `message`, etc.

---

## 7. Audit Callbacks

Both `on_success_callback` and `on_failure_callback` are attached to every task:

```python
audit_success  → audit_task_result(context, "COMPLETED")
audit_failure  → audit_task_result(context, "FAILED")
```

### What `audit_task_result` does
1. Pulls run context from XCom (`generate_run_context`).
2. Collects task metadata from `task.params`:
   - `step_number`, `task_name`, `service_name`, `environment`, `layer`, `operation`, `source_system`, `source_type`
3. Computes timing: `start_time`, `end_time`, `duration_seconds`.
4. Builds error info if status is `FAILED` (reads `context["exception"]`).
5. Pulls task results from XCom (task-specific).
6. Builds an INSERT statement and executes it via `SnowflakeHook`.

### Audit Record Fields
```python
RUN_ID,
PROJECT_NAME,
PIPELINE_NAME,
DAG_ID,
STEP_NUMBER,
TASK_ID,
TASK_NAME,
SERVICE_NAME,
ENVIRONMENT,
SOURCE_SYSTEM,
SOURCE_TYPE,
SOURCE_PATH,
FILE_NAME,
LAYER,
OPERATION,
STATUS,
ROW_COUNT,
ROWS_PARSED,
ROWS_LOADED,
ERRORS_SEEN,
MESSAGE,
START_TIME,
END_TIME,
DURATION_SECONDS,
ERROR_CODE,
ERROR_MESSAGE,
SNOWFLAKE_QUERY_ID
```

---

## 8. Task Parameters (per task)

### Task 1 — `generate_run_context`
| param | value |
|-------|-------|
| `step_number` | `1` |
| `task_name` | `Generate pipeline execution context` |
| `service_name` | `AIRFLOW` |
| `environment` | `DEV` |
| `layer` | `PIPELINE` |
| `operation` | `GENERATE_RUN_CONTEXT` |

### Task 2 — `check_s3_file`
| param | value |
|-------|-------|
| `step_number` | `2` |
| `task_name` | `Check claims input file in S3` |
| `service_name` | `AWS_S3` |
| `environment` | `DEV` |
| `source_system` | `AWS` |
| `source_type` | `S3` |
| `layer` | `LANDING` |
| `operation` | `FILE_CHECK` |

### Task 3 — `load_raw`
| param | value |
|-------|-------|
| `step_number` | `3` |
| `task_name` | `Load claims file from S3 into RAW` |
| `service_name` | `SNOWFLAKE` |
| `environment` | `DEV` |
| `source_system` | `AWS` |
| `source_type` | `S3` |
| `layer` | `RAW` |
| `operation` | `COPY_INTO` |

---

## 9. Required Airflow Connections

| Connection ID | Type | Used By |
|---------------|------|---------|
| `aws_claims` | `aws` | `check_s3_file` (S3Hook) |
| `snowflake_claims` | `snowflake` | `load_raw` + audit callback |

> Connections are configured in the Airflow UI (Admin → Connections) or via CLI. See [04-runbook.md](./04-runbook.md) for setup examples.

---

## 10. Known Improvements / Recommendations

- **Import fix**: `from airflow.providers.standard.operators.python import PythonOperator` should be `from airflow.operators.python import PythonOperator`.
- **Retries**: Add `default_args` with `retries`, `retry_delay`, `execution_timeout`.
- **Config centralization**: Move `S3_BUCKET`/`S3_PREFIX`/`ENVIRONMENT` to Airflow Variables.
- **Proper logging**: Replace `print()` with `logging.getLogger(__name__)`.
- **TaskFlow API**: Consider migrating to `@task` decorators for cleaner, type-safe code.
- **Native operators**: Consider `S3KeySensor` and `SnowflakeOperator`.