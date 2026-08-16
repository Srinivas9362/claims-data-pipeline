# End-to-End (E2E) Overview — Claims Data Pipeline

## What this E2E is

This repository contains an Airflow-based claims ingestion pipeline whose purpose is to perform an end-to-end flow from a CSV file in S3 into Snowflake RAW storage and to record audit events for each pipeline step.

The E2E flow covers:
- generation of a pipeline run context (run id, file name, S3 location)
- checking the existence of the input file in S3
- loading the CSV into Snowflake using a COPY INTO command into the RAW table
- inserting audit records into an audit table for each task (success or failure)

Why it exists: to automate and validate the ingestion of claims data into Snowflake while keeping a robust audit trail for observability and troubleshooting.

## Key components and files

- Airflow DAG: defines the pipeline tasks and dependencies
  - [claims_pipeline.py](D:/Snowflake_Apache_Aiflow_projects_2026/claims-data-pipeline/airflow/dags/claims_pipeline.py)

- Sample input data (used for local/manual testing):
  - [claims_20260808.csv](D:/Snowflake_Apache_Aiflow_projects_2026/claims-data-pipeline/data/sample/claims_20260808.csv)

- Local orchestration (docker compose) for running Airflow services
  - [docker-compose.yml](D:/Snowflake_Apache_Aiflow_projects_2026/claims-data-pipeline/docker-compose.yml)

- Snowflake validation script (connects to Snowflake and checks RAW table count)
  - [test_snowflake.py](D:/Snowflake_Apache_Aiflow_projects_2026/claims-data-pipeline/scripts/test_snowflake.py)

- Airflow runtime artifacts and logs
  - DAGs and logs folder: [airflow/] (D:/Snowflake_Apache_Aiflow_projects_2026/claims-data-pipeline/airflow)

- Environment secrets (local values used by docker-compose / Airflow):
  - [.env](D:/Snowflake_Apache_Aiflow_projects_2026/claims-data-pipeline/.env)

## High-level task flow (from the DAG)

The DAG (claims_pipeline) contains three main PythonOperator tasks executed sequentially:

1. generate_run_context
   - Builds run metadata: run_id, file_date (default is Airflow logical date or configured via dag_run.conf), file_name (claims_<file_date>.csv), s3_bucket and s3_key.
   - Returns these values via XCom.

2. check_s3_file
   - Reads the run context from XCom, uses an S3Hook to verify the presence of the file at s3://{s3_bucket}/{s3_key}.
   - Fails (FileNotFoundError) if the file is missing.

3. load_raw
   - Uses SnowflakeHook to run a COPY INTO statement from the Snowflake S3 stage into CLAIMS_DATA_DB.RAW.CLAIMS_RAW.
   - Pushes a result XCom with load status, rows parsed/loaded, errors and Snowflake query id.
   - Raises a RuntimeError if the COPY reports failure.

Auditing:
- Each task calls an audit callback on success or failure that inserts a record into PLATFORM_AUDIT_DB.AUDIT.PIPELINE_AUDIT with run/task metadata and result details.

## Expected inputs and outputs

Input:
- A CSV file named claims_YYYYMMDD.csv located at S3 path: s3://claims-data-pipeline-2026-0808/claims-data-pipeline/input/ (configured by DAG variables S3_BUCKET and S3_PREFIX).
- Example local test file: [data/sample/claims_20260808.csv](D:/Snowflake_Apache_Aiflow_projects_2026/claims-data-pipeline/data/sample/claims_20260808.csv)

Primary outputs:
- Snowflake table: CLAIMS_DATA_DB.RAW.CLAIMS_RAW — the rows loaded by the COPY command.
- Audit table: PLATFORM_AUDIT_DB.AUDIT.PIPELINE_AUDIT — one row per task execution with status and metrics.
- Airflow task logs in the repo at [airflow/logs] (D:/Snowflake_Apache_Aiflow_projects_2026/claims-data-pipeline/airflow/logs).

## How to run the pipeline locally (recommended for E2E testing)

Prerequisites:
- Docker and Docker Compose installed.
- A reachable Snowflake account with a stage configured (the DAG expects a Snowflake stage `@CLAIMS_DATA_DB.RAW.CLAIMS_S3_STAGE/input/` and credentials via Airflow connection `snowflake_claims`).
- AWS S3 access for the bucket `claims-data-pipeline-2026-0808`, or configure Snowflake stage to point to a location where the COPY can access the CSV.

1. Start Airflow services:
   - docker compose up -d
   - This will start Postgres and the Airflow images defined in [docker-compose.yml](D:/Snowflake_Apache_Aiflow_projects_2026/claims-data-pipeline/docker-compose.yml).

2. Place the sample CSV where Snowflake can access it:
   - Option A (preferred for true S3 flow): upload `data/sample/claims_20260808.csv` to the configured S3 bucket/prefix so it is available as `claims_20260808.csv`.
   - Option B (if using Snowflake local stage): configure the Snowflake stage `CLAIMS_DATA_DB.RAW.CLAIMS_S3_STAGE` to point to a storage location where the file exists.

3. Trigger the DAG manually with a specific file_date (useful for testing):
   - With docker-compose: docker compose exec airflow-api-server airflow dags trigger claims_pipeline -c "{\"file_date\": \"20260808\"}"
   - Or trigger via the Airflow UI at http://localhost:8080 and pass `{"file_date":"20260808"}` as DAG run configuration.

4. Monitor logs:
   - Check Airflow logs under [airflow/logs] (D:/Snowflake_Apache_Aiflow_projects_2026/claims-data-pipeline/airflow/logs) or use the Airflow UI logs for each task.

5. Validate the Snowflake load using the provided script:
   - Set the Snowflake environment variables expected by `scripts/test_snowflake.py` (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_ROLE).
   - Run: python scripts/test_snowflake.py
   - This will print account info and attempt to SELECT COUNT(*) FROM CLAIMS_DATA_DB.RAW.CLAIMS_RAW.

## Common troubleshooting tips

- FileNotFoundError in check_s3_file: confirm the file name and S3 key match the generated run context. The DAG uses a `file_date` derived from the logical date or the run config. For manual runs, pass file_date in dag_run.conf to force the exact file name.

- Snowflake COPY errors: inspect the COPY result printed in the load_raw task log. The DAG expects the Snowflake COPY to return a result row with columns describing rows parsed/loaded and any errors — the DAG fails if Snowflake reports a non-LOADED status.

- Audit records missing: ensure the DAG's Snowflake connection `snowflake_claims` is correctly configured and the audit table `PLATFORM_AUDIT_DB.AUDIT.PIPELINE_AUDIT` exists and is writable.

- Airflow logs: see [airflow/logs] for task-specific logs. The DAG prints friendly structured logs for run context, file checks, COPY results and audit outcomes.

## Security and configuration notes

- Credentials are expected to be provided as Airflow connections (AWS and Snowflake) and environment variables. Do NOT commit secrets to the repository.
- The sample `.env` file contains only Airflow secrets for the local Airflow instance. Snowflake and AWS credentials must be set up separately in the environment or Airflow connections.

## Quick file references

- DAG: [airflow/dags/claims_pipeline.py](D:/Snowflake_Apache_Aiflow_projects_2026/claims-data-pipeline/airflow/dags/claims_pipeline.py)
- Sample data: [data/sample/claims_20260808.csv](D:/Snowflake_Apache_Aiflow_projects_2026/claims-data-pipeline/data/sample/claims_20260808.csv)
- Docker compose: [docker-compose.yml](D:/Snowflake_Apache_Aiflow_projects_2026/claims-data-pipeline/docker-compose.yml)
- Snowflake test: [scripts/test_snowflake.py](D:/Snowflake_Apache_Aiflow_projects_2026/claims-data-pipeline/scripts/test_snowflake.py)
- Airflow logs folder: [airflow/logs](D:/Snowflake_Apache_Aiflow_projects_2026/claims-data-pipeline/airflow/logs)

---

If desired, a follow-up can add a small runbook with exact commands for uploading the sample CSV to S3 (AWS CLI commands) and example Airflow connection configuration snippets for Snowflake/AWS. Also can add a simple integration test that automates upload → DAG trigger → validation using the test_snowflake.py script.
