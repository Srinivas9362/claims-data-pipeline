# Claims Data Pipeline — Runbook (Setup, Run, Troubleshoot)

## 1. Prerequisites

| Requirement | Version / Detail |
|-------------|------------------|
| Docker | Docker Engine + Docker Compose |
| Airflow image | `apache/airflow:3.0.2` |
| PostgreSQL image | `postgres:16` |
| Snowflake account | Reachable from local machine |
| AWS S3 bucket | `claims-data-pipeline-2026-0808` (or configured) |
| AWS credentials | Required for both S3Hook and Snowflake external stage |
| Python 3 | For `scripts/test_snowflake.py` (or use a venv) |

---

## 2. One-Time Infrastructure Setup

### 2.1 AWS S3

1. Create the bucket (if not exists):
   ```bash
   aws s3 mb s3://claims-data-pipeline-2026-0808
   ```

2. Create the input prefix:
   ```bash
   aws s3api put-object \
     --bucket claims-data-pipeline-2026-0808 \
     --key claims-data-pipeline/input/
   ```

3. Upload the sample file:
   ```bash
   aws s3 cp data/sample/claims_20260808.csv \
     s3://claims-data-pipeline-2026-0808/claims-data-pipeline/input/claims_20260808.csv
   ```

4. Set up a Snowflake **storage integration** that grants Snowflake read access to this bucket. Example:
   ```sql
   CREATE STORAGE INTEGRATION claims_s3_int
     TYPE = EXTERNAL_STAGE
     STORAGE_PROVIDER = 'S3'
     ENABLED = TRUE
     STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT>:role/<ROLE>'
     STORAGE_ALLOWED_LOCATIONS = ('s3://claims-data-pipeline-2026-0808/');
   ```

### 2.2 Snowflake Objects

Connect to Snowflake (e.g., via `snowsql`) and create:

```sql
-- Warehouse / database / schema
CREATE WAREHOUSE IF NOT EXISTS CLAIMS_WH
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS CLAIMS_DATA_DB;
CREATE SCHEMA IF NOT EXISTS CLAIMS_DATA_DB.RAW;

CREATE DATABASE IF NOT EXISTS PLATFORM_AUDIT_DB;
CREATE SCHEMA IF NOT EXISTS PLATFORM_AUDIT_DB.AUDIT;

-- RAW claims table (match your CSV columns)
CREATE OR REPLACE TABLE CLAIMS_DATA_DB.RAW.CLAIMS_RAW (
    claim_id        VARCHAR(20),
    patient_id      VARCHAR(20),
    claim_amount    NUMBER(12, 2),
    claim_status    VARCHAR(20),
    claim_date      DATE
);

-- External stage pointing to S3
CREATE OR REPLACE STAGE CLAIMS_DATA_DB.RAW.CLAIMS_S3_STAGE
  URL = 's3://claims-data-pipeline-2026-0808/'
  STORAGE_INTEGRATION = claims_s3_int
  DIRECTORY = (ENABLE = TRUE);

-- Audit table
CREATE OR REPLACE TABLE PLATFORM_AUDIT_DB.AUDIT.PIPELINE_AUDIT (
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

---

## 3. Start Airflow (Docker Compose)

```bash
# From the project root
docker compose up -d
```

This starts:
1. `postgres` — metadata DB (waits until healthy)
2. `airflow-init` — runs `db migrate` then exits
3. `airflow-api-server` — Web UI + API on `http://localhost:8080`
4. `airflow-scheduler` — executes tasks
5. `airflow-dag-processor` — syncs DAG files

### Verify services
```bash
docker compose ps
```

### Check init completed
```bash
docker compose logs airflow-init
```

---

## 4. Configure Airflow Connections

Connections are required before running the DAG.

### Option A — Airflow CLI

```bash
# Inside the api-server container
docker compose exec airflow-api-server airflow connections add \
  --conn-type aws \
  --conn-id aws_claims \
  --conn-extra '{"aws_access_key_id": "YOUR_KEY", "aws_secret_access_key": "YOUR_SECRET"}' \
  aws_claims

docker compose exec airflow-api-server airflow connections add \
  --conn-type snowflake \
  --conn-id snowflake_claims \
  --conn-extra '{"account": "YOUR_ACCOUNT", "warehouse": "CLAIMS_WH", "database": "CLAIMS_DATA_DB", "schema": "RAW", "role": "YOUR_ROLE"}' \
  --conn-login YOUR_USER \
  --conn-password YOUR_PASSWORD \
  snowflake_claims
```

### Option B — Airflow UI
1. Open `http://localhost:8080` → login (use the generated password — see `README.md` or:
   `docker compose exec airflow-api-server cat /opt/airflow/simple_auth_manager_passwords.json.generated`)
2. **Admin → Connections → Add**
   - `aws_claims` (AWS type) with access key / secret
   - `snowflake_claims` (Snowflake type) with account, login, password, schema, warehouse, role

---

## 5. Trigger the DAG (Manual Run)

### Via CLI (with `file_date` override)
```bash
docker compose exec airflow-api-server airflow dags trigger claims_pipeline \
  -c '{"file_date": "20260808"}'
```

### Via Airflow UI
1. Open `http://localhost:8080`
2. Click **DAGs** → `claims_pipeline`
3. Click **Trigger DAG** (play button)
4. Optional: add run config `{"file_date": "20260808"}`

### Without override
If no config is passed, the DAG uses the **logical date** of the manual run to build the file name.

---

## 6. Monitor the Run

### Airflow UI
- DAG runs view: status of each run
- Task instances: expand each task to see logs / XCom values
- Logs: click a task → **Log** tab

### CLI
```bash
# List recent DAG runs
docker compose exec airflow-api-server airflow dags list-runs -d claims_pipeline

# Task instances for a run
docker compose exec airflow-api-server airflow tasks list claims_pipeline

# Check task state
docker compose exec airflow-api-server airflow tasks states-for-dag-run claims_pipeline \
  <RUN_ID>
```

### Local log files
```
airflow/logs/dag_id=claims_pipeline/
```

---

## 7. Validate the Data Load

### Option A — Python script
```bash
# Set environment variables (Windows PowerShell):
$env:SNOWFLAKE_ACCOUNT="YOUR_ACCOUNT"
$env:SNOWFLAKE_USER="YOUR_USER"
$env:SNOWFLAKE_PASSWORD="YOUR_PASSWORD"
$env:SNOWFLAKE_WAREHOUSE="CLAIMS_WH"
$env:SNOWFLAKE_DATABASE="CLAIMS_DATA_DB"
$env:SNOWFLAKE_SCHEMA="RAW"
$env:SNOWFLAKE_ROLE="YOUR_ROLE"

python scripts/test_snowflake.py
```

Expected output:
```
Snowflake connection successful!
--------------------------------
Account    : ...
Region     : ...
User       : ...
Role       : ...
Database   : ...
Warehouse  : ...
RAW record count: 5
```

### Option B — Snowflake SQL
```sql
SELECT * FROM CLAIMS_DATA_DB.RAW.CLAIMS_RAW;
SELECT COUNT(*) FROM CLAIMS_DATA_DB.RAW.CLAIMS_RAW;

-- Check audit trail
SELECT * FROM PLATFORM_AUDIT_DB.AUDIT.PIPELINE_AUDIT
ORDER BY START_TIME DESC;
```

---

## 8. Common Troubleshooting

### 8.1 DAG not visible in Airflow UI
- Check `docker compose logs airflow-dag-processor`
- Verify the DAG file is at `airflow/dags/claims_pipeline.py`
- Check for Python import errors:
  ```
  docker compose exec airflow-api-server airflow dags list
  ```

**Known issue:** The DAG uses
`from airflow.providers.standard.operators.python import PythonOperator`
This package does not exist — fix to
`from airflow.operators.python import PythonOperator`.

### 8.2 `Invalid connection id` / connection not found
- Verify connections exist:
  ```bash
  docker compose exec airflow-api-server airflow connections list
  ```

### 8.3 `FileNotFoundError` in `check_s3_file`
- Confirm the file is at:
  `s3://claims-data-pipeline-2026-0808/claims-data-pipeline/input/claims_<date>.csv`
- Confirm the S3 connection (`aws_claims`) has correct credentials.

### 8.4 `0 files processed` in `load_raw`
- Snowflake may have already loaded the file (dedupe: COPY tracks processed files).
- To force re-load, use `FORCE = TRUE` in the COPY (or provide a new file name).
- Check `COPY INTO` history:
  ```sql
  SELECT *
  FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
      TABLE_NAME => 'CLAIMS_DATA_DB.RAW.CLAIMS_RAW',
      START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())
  ));
  ```

### 8.5 Snowflake `Access denied` / permission errors
- Verify storage integration + IAM role policy.
- Test listing stage:
  ```sql
  LIST @CLAIMS_DATA_DB.RAW.CLAIMS_S3_STAGE/input/;
  ```

### 8.6 Audit records missing
- Confirm `PLATFORM_AUDIT_DB.AUDIT.PIPELINE_AUDIT` exists and is writable.
- Check the audit callback errors in task logs.
- Ensure `snowflake_claims` connection has INSERT privilege.

### 8.7 Reset / restart all services
```bash
docker compose down -v    # ⚠️ Wipes Postgres volume (metadata DB)
docker compose up -d
```

---

## 9. Sample End-to-End Walkthrough

1. **Upload file**
   ```bash
   aws s3 cp data/sample/claims_20260808.csv \
     s3://claims-data-pipeline-2026-0808/claims-data-pipeline/input/claims_20260808.csv
   ```

2. **Start Airflow**
   ```bash
   docker compose up -d
   ```

3. **Set up connections** (once): see section 4.

4. **Trigger DAG**
   ```bash
   docker compose exec airflow-api-server airflow dags trigger claims_pipeline \
     -c '{"file_date": "20260808"}'
   ```

5. **Wait ~30–60s** then check status in UI or CLI.

6. **Validate**
   ```bash
   python scripts/test_snowflake.py
   ```

7. **Check audit**
   ```sql
   SELECT RUN_ID, STEP_NUMBER, TASK_ID, STATUS, ROWS_LOADED, DURATION_SECONDS
   FROM PLATFORM_AUDIT_DB.AUDIT.PIPELINE_AUDIT
   ORDER BY START_TIME DESC;
   ```

Expected: 3 audit rows (one per task), all `COMPLETED`, `ROWS_LOADED = 5`.

---

## 10. Reference Commands

| Operation | Command |
|-----------|---------|
| Start services | `docker compose up -d` |
| Stop services | `docker compose down` |
| Stop + wipe metadata DB | `docker compose down -v` |
| View logs (api-server) | `docker compose logs -f airflow-api-server` |
| View logs (scheduler) | `docker compose logs -f airflow-scheduler` |
| DAG list | `docker compose exec airflow-api-server airflow dags list` |
| Connections list | `docker compose exec airflow-api-server airflow connections list` |
| Trigger DAG | `docker compose exec airflow-api-server airflow dags trigger claims_pipeline` |