# Claims Data Pipeline — Snowflake Schema & Objects

## 1. Overview

This pipeline uses two Snowflake databases:

1. **CLAIMS_DATA_DB** — holds the landing (RAW) claims data.
2. **PLATFORM_AUDIT_DB** — holds pipeline execution audit records.

All data movement is handled natively by Snowflake via an **external S3 stage** — no data passes through Airflow memory.

---

## 2. Database / Schema Layout

```
CLAIMS_DATA_DB
└── RAW
    ├── CLAIMS_RAW          (table — landed CSV rows)
    └── CLAIMS_S3_STAGE     (stage — points to S3 input prefix)

PLATFORM_AUDIT_DB
└── AUDIT
    └── PIPELINE_AUDIT      (table — one row per task execution)
```

---

## 3. Table: `CLAIMS_DATA_DB.RAW.CLAIMS_RAW`

### Purpose
Stores the raw claims rows loaded via `COPY INTO` from the S3 stage.

### Inferred Schema (from sample CSV)
| Column | Type (inferred) | Sample |
|--------|-----------------|--------|
| `claim_id` | VARCHAR / STRING | `C001` |
| `patient_id` | VARCHAR / STRING | `P001` |
| `claim_amount` | NUMBER(10,2) | `1200.50` |
| `claim_status` | VARCHAR / STRING | `APPROVED` |
| `claim_date` | DATE | `2026-08-01` |

### Sample Data (from `data/sample/claims_20260808.csv`)
| claim_id | patient_id | claim_amount | claim_status | claim_date |
|----------|------------|--------------|--------------|------------|
| C001     | P001       | 1200.50      | APPROVED     | 2026-08-01 |
| C002     | P002       | 850.00       | PENDING      | 2026-08-02 |
| C003     | P003       | 2400.75      | APPROVED     | 2026-08-03 |
| C004     | P004       | 450.25       | REJECTED     | 2026-08-04 |
| C005     | P005       | 1750.00      | APPROVED     | 2026-08-05 |

> **Note:** The exact column types depend on the DDL used to create `CLAIMS_RAW`. The table should be created to match the CSV columns (plus optional metadata columns if Snowflake's `COPY INTO` appends any, e.g., a file/load timestamp, if configured).

---

## 4. Stage: `CLAIMS_DATA_DB.RAW.CLAIMS_S3_STAGE`

### Purpose
External stage pointing to the S3 input location so Snowflake can read the CSV files directly.

### Expected Configuration
```sql
CREATE OR REPLACE STAGE CLAIMS_DATA_DB.RAW.CLAIMS_S3_STAGE
  URL = 's3://claims-data-pipeline-2026-0808/'
  STORAGE_INTEGRATION = <your_s3_storage_integration>
  DIRECTORY = (ENABLE = TRUE);
```

> The DAG references the stage with a sub-path: `@CLAIMS_DATA_DB.RAW.CLAIMS_S3_STAGE/input/`

### File Format (inline in DAG)
```sql
TYPE = CSV
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
```

---

## 5. The COPY INTO Statement (from DAG)

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

### Behavior Notes
| Option | Value | Effect |
|--------|-------|--------|
| `PATTERN` | `.*<file_name>` | Only loads the exact input file for the run |
| `ON_ERROR` | `ABORT_STATEMENT` | Fails the statement if any row errors |
| `FILE_FORMAT` | CSV, skip header, optional quotes | Matches the sample CSV structure |

### COPY Result Columns (used by DAG)
| Index | Column | DAG variable |
|-------|--------|--------------|
| 0 | file name | `loaded_file_name` |
| 1 | status | `copy_status` |
| 2 | rows parsed | `rows_parsed` |
| 3 | rows loaded | `rows_loaded` |
| 5 | errors seen | `errors_seen` |
| 6 | first error | `first_error` |
| 7 | first error line | `first_error_line` |
| 8 | first error character | `first_error_character` |
| 9 | first error column name | `first_error_column_name` |

---

## 6. Table: `PLATFORM_AUDIT_DB.AUDIT.PIPELINE_AUDIT`

### Purpose
Stores one audit record per task execution (success or failure) for observability and troubleshooting.

### Columns
| Column | Type (inferred) | Description |
|--------|-----------------|-------------|
| `RUN_ID` | VARCHAR(20) | Pipeline run identifier (`YYYYMMDDHHMMSS`) |
| `PROJECT_NAME` | VARCHAR | `CLAIMS_PIPELINE` |
| `PIPELINE_NAME` | VARCHAR | `claims_pipeline` |
| `DAG_ID` | VARCHAR | `claims_pipeline` |
| `STEP_NUMBER` | INT | 1, 2, or 3 |
| `TASK_ID` | VARCHAR | `generate_run_context` / `check_s3_file` / `load_raw` |
| `TASK_NAME` | VARCHAR | Human-readable task description |
| `SERVICE_NAME` | VARCHAR | `AIRFLOW` / `AWS_S3` / `SNOWFLAKE` |
| `ENVIRONMENT` | VARCHAR | `DEV` (or configured) |
| `SOURCE_SYSTEM` | VARCHAR | e.g. `AWS` |
| `SOURCE_TYPE` | VARCHAR | e.g. `S3` |
| `SOURCE_PATH` | VARCHAR | `s3://...` full URI |
| `FILE_NAME` | VARCHAR | e.g. `claims_20260808.csv` |
| `LAYER` | VARCHAR | `PIPELINE` / `LANDING` / `RAW` |
| `OPERATION` | VARCHAR | `GENERATE_RUN_CONTEXT` / `FILE_CHECK` / `COPY_INTO` |
| `STATUS` | VARCHAR | `COMPLETED` / `FAILED` |
| `ROW_COUNT` | INT | Num rows loaded (for load task) |
| `ROWS_PARSED` | INT | From COPY result |
| `ROWS_LOADED` | INT | From COPY result |
| `ERRORS_SEEN` | INT | From COPY result |
| `MESSAGE` | VARCHAR | Task outcome message |
| `START_TIME` | TIMESTAMP | Task start |
| `END_TIME` | TIMESTAMP | Task end |
| `DURATION_SECONDS` | FLOAT | Elapsed time |
| `ERROR_CODE` | VARCHAR | Exception errno (if any) |
| `ERROR_MESSAGE` | VARCHAR | Exception message (if any) |
| `SNOWFLAKE_QUERY_ID` | VARCHAR | Query ID of the COPY (load_raw only) |

---

## 7. EXAMPLE INSERT (for creating/test the audit table)

Approximate DDL:
```sql
CREATE TABLE IF NOT EXISTS PLATFORM_AUDIT_DB.AUDIT.PIPELINE_AUDIT (
    RUN_ID                VARCHAR(20),
    PROJECT_NAME          VARCHAR(100),
    PIPELINE_NAME         VARCHAR(100),
    DAG_ID                VARCHAR(100),
    STEP_NUMBER           INT,
    TASK_ID               VARCHAR(100),
    TASK_NAME             VARCHAR(200),
    SERVICE_NAME          VARCHAR(100),
    ENVIRONMENT           VARCHAR(20),
    SOURCE_SYSTEM         VARCHAR(100),
    SOURCE_TYPE           VARCHAR(100),
    SOURCE_PATH           VARCHAR(500),
    FILE_NAME             VARCHAR(200),
    LAYER                 VARCHAR(50),
    OPERATION             VARCHAR(100),
    STATUS                VARCHAR(20),
    ROW_COUNT             INT,
    ROWS_PARSED           INT,
    ROWS_LOADED           INT,
    ERRORS_SEEN           INT,
    MESSAGE               VARCHAR(1000),
    START_TIME            TIMESTAMP_NTZ,
    END_TIME              TIMESTAMP_NTZ,
    DURATION_SECONDS      FLOAT,
    ERROR_CODE            VARCHAR(100),
    ERROR_MESSAGE         VARCHAR(2000),
    SNOWFLAKE_QUERY_ID    VARCHAR(100)
);
```

> ⚠️ Verify actual DDL against your Snowflake account — the exact types/lengths may differ in your environment.

---

## 8. Validation Query (from `scripts/test_snowflake.py`)

```python
# Connection info (from env vars):
#   SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
#   SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA,
#   SNOWFLAKE_ROLE

# Step 1 — connectivity check
SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_USER(),
       CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_WAREHOUSE();

# Step 2 — raw record count
SELECT COUNT(*) FROM CLAIMS_DATA_DB.RAW.CLAIMS_RAW;
```

### Useful Audit Queries
```sql
-- Most recent runs
SELECT RUN_ID, STEP_NUMBER, TASK_ID, STATUS, START_TIME, DURATION_SECONDS
FROM PLATFORM_AUDIT_DB.AUDIT.PIPELINE_AUDIT
ORDER BY START_TIME DESC
LIMIT 30;

-- Failed tasks
SELECT RUN_ID, STEP_NUMBER, TASK_ID, ERROR_MESSAGE
FROM PLATFORM_AUDIT_DB.AUDIT.PIPELINE_AUDIT
WHERE STATUS = 'FAILED'
ORDER BY START_TIME DESC;

-- Load statistics per run
SELECT RUN_ID, FILE_NAME, ROWS_PARSED, ROWS_LOADED, ERRORS_SEEN
FROM PLATFORM_AUDIT_DB.AUDIT.PIPELINE_AUDIT
WHERE TASK_ID = 'load_raw'
ORDER BY START_TIME DESC;
```

---

## 9. Common Snowflake Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| `0 files processed` | File already loaded (Snowflake dedupe) | Check COPY history; file may be loaded already |
| `File not found` errors in COPY | Wrong stage path / file not in S3 | Verify `@.../input/` path and S3 contents |
| `Access denied` on stage | Missing storage integration / IAM permissions | Configure `STORAGE_INTEGRATION`, bucket policy |
| `PATTERN` doesn't match | Escaping issue / wrong file name | Test regex in Snowflake; verify `file_name` |
| Row errors | CSV format mismatch | Use `VALIDATION_MODE = 'RETURN_ERRORS'` to diagnose |

---

## 10. Next Steps

- Add **metadata columns** to `CLAIMS_RAW` (e.g., `LOADED_AT`, `FILE_NAME`) using Snowflake's `COPY INTO` column list or a post-load `UPDATE`.
- Add a **cleansed / conformed layer** (e.g., `CLAIMS_DATA_DB.CLEANSED`) downstream of RAW.
- Add **Snowflake pipeline/stream objects** for incremental processing instead of full-file reloads.