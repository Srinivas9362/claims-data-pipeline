# Claims Data Pipeline — Documentation

A comprehensive documentation set for the **Claims Data Pipeline** — an Apache Airflow + Amazon S3 + Snowflake E2E ingestion pipeline.

---

## 📚 Documentation Index

| # | Document | Description |
|---|----------|-------------|
| 1 | [**Architecture Overview**](./01-architecture-overview.md) | System context, high-level architecture diagram, component breakdown, data flow, key design decisions, file map. |
| 2 | [**Airflow Pipeline**](./02-airflow-pipeline.md) | DAG metadata, 3-task flow, per-task logic, XCom inputs/outputs, audit callbacks, required connections. |
| 3 | [**Snowflake Schema**](./03-snowflake-schema.md) | Databases, tables, stage, COPY statement, audit table columns, validation queries, troubleshooting. |
| 4 | [**Runbook**](./04-runbook.md) | Infrastructure setup, Airflow startup, connection config, trigger steps, monitoring, validation, troubleshooting, E2E walkthrough. |
| 5 | [**Security & Best Practices**](./05-security-and-best-practices.md) | Secret management, network access, data protection, audit, DAG code best practices, production hardening checklist. |

---

## 🎯 Quick Start (TL;DR)

```bash
# 1. Start Airflow
docker compose up -d

# 2. Trigger the DAG with a specific file_date
docker compose exec airflow-api-server airflow dags trigger claims_pipeline \
  -c '{"file_date": "20260808"}'

# 3. Validate Snowflake load
python scripts/test_snowflake.py
```

> ⚠️ Before running: ensure S3 file exists, Snowflake objects (stage, tables) exist, and Airflow connections (`aws_claims`, `snowflake_claims`) are configured. See the [Runbook](./04-runbook.md).

---

## 🔗 Related Files

| Path | Purpose |
|------|---------|
| `airflow/dags/claims_pipeline.py` | The DAG definition |
| `docker-compose.yml` | Airflow + Postgres orchestration |
| `data/sample/claims_20260808.csv` | Sample input data |
| `scripts/test_snowflake.py` | Snowflake validation script |
| `E2E_OVERVIEW.md` | Concise project overview (root) |

---

## 🧭 Document Relationships

```
01-architecture-overview.md   ←  start here (big picture)
        │
        ├──► 02-airflow-pipeline.md   ←  DAG details
        ├──► 03-snowflake-schema.md   ←  data layer
        ├──► 04-runbook.md            ←  how to run
        └──► 05-security-and-best-practices.md  ←  hardening