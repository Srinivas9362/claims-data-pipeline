# Claims Data Pipeline — Architecture Overview

## 1. System Context

This project implements an **end-to-end (E2E) claims ingestion pipeline** that moves healthcare claims data from an **Amazon S3** landing zone into a **Snowflake** RAW layer, while maintaining a **full audit trail** for every pipeline step.

The pipeline is orchestrated by **Apache Airflow 3.0.2** running locally via **Docker Compose**.

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SOURCE SYSTEMS                                │
│                                                                             │
│   ┌─────────────────┐        ┌──────────────────────────────────────────┐   │
│   │  Claims Producer │ ─────► │  Amazon S3 (Landing Zone)               │   │
│   │  (CSV Files)     │        │  s3://claims-data-pipeline-2026-0808/   │   │
│   └─────────────────┘        │  claims-data-pipeline/input/             │   │
│                              │  claims_YYYYMMDD.csv                     │   │
│                              └──────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        │  (Snowflake Stage reads from S3)
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              APACHE AIRFLOW                                │
│                          (Docker Compose / Local)                          │
│                                                                             │
│   ┌────────────────┐   ┌──────────────┐   ┌─────────────────────────────┐   │
│   │  1. Generate   │──►│  2. Check    │──►│  3. Load S3 → Snowflake    │   │
│   │  Run Context   │   │  S3 File     │   │     (COPY INTO RAW)        │   │
│   └────────────────┘   └──────────────┘   └─────────────────────────────┘   │
│                                                                             │
│   Every task triggers on_success_callback / on_failure_callback            │
│                     ▼                                                       │
│            ┌──────────────────┐                                            │
│            │  Audit Callback  │                                            │
│            └──────────────────┘                                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SNOWFLAKE                                      │
│                                                                             │
│   ┌─────────────────────────────┐   ┌───────────────────────────────────┐  │
│   │ CLAIMS_DATA_DB.RAW          │   │ PLATFORM_AUDIT_DB.AUDIT           │  │
│   │  .CLAIMS_RAW (landed data)  │   │  .PIPELINE_AUDIT (audit records)  │  │
│   └─────────────────────────────┘   └───────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Component Breakdown

### 3.1 Source: Amazon S3

| Property | Value |
|----------|-------|
| **Bucket** | `claims-data-pipeline-2026-0808` |
| **Prefix** | `claims-data-pipeline/input/` |
| **File pattern** | `claims_YYYYMMDD.csv` |
| **Example** | `claims_20260808.csv` |

The input CSV contains healthcare claim records with the following schema:

| Column | Sample Value |
|--------|--------------|
| `claim_id` | `C001` |
| `patient_id` | `P001` |
| `claim_amount` | `1200.50` |
| `claim_status` | `APPROVED` |
| `claim_date` | `2026-08-01` |

### 3.2 Orchestrator: Apache Airflow

- **Version**: 3.0.2 (`apache/airflow:3.0.2`)
- **Executor**: `LocalExecutor`
- **Deployment**: Docker Compose (local machine)
- **Services**:
  - `postgres` — Airflow metadata database (PostgreSQL 16)
  - `airflow-init` — Runs DB migration on first startup
  - `airflow-api-server` — REST API + Web UI (port `8080`)
  - `airflow-scheduler` — Schedules and executes tasks
  - `airflow-dag-processor` — Parses and syncs DAG files

Read more: [02-airflow-pipeline.md](./02-airflow-pipeline.md)

### 3.3 Target: Snowflake

| Object | Purpose |
|--------|---------|
| `CLAIMS_DATA_DB.RAW.CLAIMS_RAW` | Raw landed claims data (result of `COPY INTO`) |
| `CLAIMS_DATA_DB.RAW.CLAIMS_S3_STAGE` | External stage pointing at S3 input location |
| `PLATFORM_AUDIT_DB.AUDIT.PIPELINE_AUDIT` | One audit row per task run (success or failure) |

Read more: [03-snowflake-schema.md](./03-snowflake-schema.md)

---

## 4. Data Flow (E2E)

```
CSV file lands in S3
        │
        ▼
1. generate_run_context
   - Builds run_id, file_date, file_name, s3_bucket, s3_key
   - Pushes dict to XCom
        │
        ▼
2. check_s3_file
   - Reads run context from XCom
   - Uses S3Hook to verify file exists
   - Fails if file not found
        │
        ▼
3. load_raw
   - Reads run context from XCom
   - Runs Snowflake COPY INTO from stage
   - Handles 0-files / empty / normal results
   - Pushes load result + query id to XCom
        │
        ▼
   (every task) ──► Audit callback inserts into PIPELINE_AUDIT
```

---

## 5. Key Design Decisions

1. **Run context as a single source of truth**
   - All downstream tasks pull the same run context dict from XCom, keeping task input/output consistent.

2. **Manual file_date override**
   - Users can pass `{"file_date": "20260808"}` in `dag_run.conf` for manual testing, falling back to the Airflow logical date.

3. **Audit via callbacks**
   - Instead of explicit audit tasks, `on_success_callback` / `on_failure_callback` record every task execution automatically.

4. **Snowflake-native loading**
   - Data movement uses Snowflake's `COPY INTO` with an external S3 stage, avoiding streaming through Airflow memory.

5. **Local-first orchestration**
   - Docker Compose provides a full Airflow environment for development and E2E validation.

---

## 6. Environment / Secrets Management

| Item | Location | Notes |
|------|----------|-------|
| Airflow JWT secret | `.env` → `AIRFLOW_JWT_SECRET` | Used by API server + scheduler |
| Airflow Fernet key | `.env` → `AIRFLOW__CORE__FERNET_KEY` | Used for connection encryption |
| AWS connection | Airflow connection `aws_claims` | S3 credentials (configured in UI/CLI) |
| Snowflake connection | Airflow connection `snowflake_claims` | Snowflake credentials (configured in UI/CLI) |
| Snowflake env vars | Environment (for `test_snowflake.py`) | `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, etc. |

> ⚠️ **Security note**: `.env` is git-ignored. Never commit real secrets. See [05-security-and-best-practices.md](./05-security-and-best-practices.md).

---

## 7. File Map

| Path | Purpose |
|------|---------|
| `airflow/dags/claims_pipeline.py` | The DAG definition (3 tasks + audit callbacks) |
| `docker-compose.yml` | Airflow + Postgres local orchestration |
| `data/sample/claims_20260808.csv` | Sample input data for testing |
| `scripts/test_snowflake.py` | Snowflake connectivity + RAW row count validator |
| `E2E_OVERVIEW.md` | Existing project overview |
| `docs/*.md` | This documentation set |

---

## 8. Next Steps / Roadmap Suggestions

- Add **S3KeySensor** instead of a custom Python `check_s3_file` task.
- Add **retries / retry_delay / alerting** via `default_args`.
- Move hardcoded config (bucket, prefix, environment) to **Airflow Variables**.
- Add **SLA** and **execution_timeout** for production visibility.
- Add **CI/CD** to automatically upload sample CSV to S3 and trigger the DAG.