# Claims Data Pipeline

An **end-to-end (E2E) claims ingestion pipeline** using **Apache Airflow**, **Amazon S3**, and **Snowflake**. It moves claims CSV files from S3 into Snowflake's RAW layer while maintaining a full audit trail.

---

## 📖 Documentation

Comprehensive documentation is available in the [`docs/`](./docs/) folder:

| Document | Description |
|----------|-------------|
| [**Architecture Overview**](./docs/01-architecture-overview.md) | System architecture, components, data flow, design decisions |
| [**Airflow Pipeline**](./docs/02-airflow-pipeline.md) | DAG tasks, logic, XCom, audit callbacks |
| [**Snowflake Schema**](./docs/03-snowflake-schema.md) | Tables, stages, COPY statement, audit schema |
| [**Runbook**](./docs/04-runbook.md) | Setup, run, monitor, troubleshoot, E2E walkthrough |
| [**Security & Best Practices**](./docs/05-security-and-best-practices.md) | Secrets, access control, hardening checklist |

Start with the **[Documentation Index](./docs/README.md)**.

---

## 🚀 Quick Start

```bash
# 1. Start Airflow services
docker compose up -d

# 2. Trigger the DAG (manual run with file_date)
docker compose exec airflow-api-server airflow dags trigger claims_pipeline \
  -c '{"file_date": "20260808"}'

# 3. Validate the Snowflake load
python scripts/test_snowflake.py
```

> ⚠️ See the [Runbook](./docs/04-runbook.md) for prerequisites (AWS S3 setup, Snowflake objects, Airflow connections).

---

## 🏗️ Architecture (Summary)

```
S3 (CSV) ──► Airflow DAG ──► Snowflake (RAW + AUDIT)
                │
        ┌───────┴────────┐
        │  3 tasks       │
        │  1. Generate run context
        │  2. Check S3 file
        │  3. Load into RAW (COPY INTO)
        └── every task writes an audit record
```

**Key components:**
- **Airflow DAG**: `airflow/dags/claims_pipeline.py` — 3 sequential tasks + audit callbacks
- **Docker Compose**: Airflow 3.0.2 (api-server, scheduler, dag-processor) + PostgreSQL 16
- **Snowflake**: `CLAIMS_DATA_DB.RAW.CLAIMS_RAW` + `PLATFORM_AUDIT_DB.AUDIT.PIPELINE_AUDIT`
- **Sample data**: `data/sample/claims_20260808.csv`

---

## 📁 Project Structure

```
claims-data-pipeline/
├── airflow/
│   ├── dags/
│   │   └── claims_pipeline.py      # The DAG definition
│   ├── logs/                        # Airflow task logs
│   └── plugins/
├── config/                          # (empty/config space)
├── data/
│   └── sample/
│       └── claims_20260808.csv      # Sample input
├── docs/                            # 📚 Documentation set
│   ├── README.md
│   ├── 01-architecture-overview.md
│   ├── 02-airflow-pipeline.md
│   ├── 03-snowflake-schema.md
│   ├── 04-runbook.md
│   └── 05-security-and-best-practices.md
├── scripts/
│   └── test_snowflake.py            # Snowflake validation
├── snowflake/                       # (empty/SQL space)
├── docker-compose.yml
├── .env                             # 🔒 git-ignored secrets
├── .gitignore
└── E2E_OVERVIEW.md                  # Concise overview
```

---

## 🔐 Accessing Airflow UI

1. Start services: `docker compose up -d`
2. Open http://localhost:8080
3. Login with the generated admin password:
   ```bash
   docker compose exec airflow-api-server cat /opt/airflow/simple_auth_manager_passwords.json.generated
   ```

---

## 🛡️ Security Note

- `.env` is git-ignored — **never commit secrets**.
- Airflow connections (`aws_claims`, `snowflake_claims`) hold the AWS/Snowflake credentials.
- See [Security & Best Practices](./docs/05-security-and-best-practices.md) for hardening guidance.

---

## 📌 Known Issues / Improvements

- The DAG import `from airflow.providers.standard.operators.python import PythonOperator` is invalid — should be `from airflow.operators.python import PythonOperator`.
- Add `default_args` (retries, timeout, alerting).
- Centralize config (S3 bucket, prefix, environment) into Airflow Variables.

See [Security & Best Practices](./docs/05-security-and-best-practices.md) for a full list.