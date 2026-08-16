# Claims Data Pipeline — Security & Best Practices

## 1. Security Posture Overview

This pipeline handles **healthcare claims data** (PII/PHI-adjacent). Security is a critical concern at every layer: source (S3), orchestration (Airflow), and target (Snowflake).

---

## 2. Secret Management

### 2.1 What lives where today

| Secret | Storage Location | Security Level |
|--------|------------------|----------------|
| `AIRFLOW_JWT_SECRET` | `.env` (git-ignored) | Local only |
| `AIRFLOW__CORE__FERNET_KEY` | `.env` (git-ignored) | Local only |
| AWS access key / secret | Airflow connection `aws_claims` | Encrypted in Airflow metadata DB (using Fernet) |
| Snowflake user / password | Airflow connection `snowflake_claims` | Encrypted in Airflow metadata DB (using Fernet) |
| Snowflake env vars (for test script) | Local environment | Plain text env vars |

### 2.2 Rules

- ✅ `.env` is in `.gitignore` — never commit it.
- ✅ Secrets are **not** hardcoded in the DAG.
- ⚠️ **Do not** copy `.env` values into documentation or commit them to git.
- ⚠️ Airflow connection passwords appear in `airflow connections list` output — restrict access to that command.
- ⚠️ Rotate the Fernet key and JWT secret before moving to shared/prod environments.

### 2.3 Recommended improvements

| Improvement | Description |
|-------------|-------------|
| **Use Airflow Variables/Secrets backend** | Store secrets in AWS Secrets Manager / HashiCorp Vault and reference via Airflow's secrets backend. |
| **Use IAM roles instead of keys** | For AWS: use instance/container roles (EKS/EC2/ECS) instead of long-lived access keys. |
| **Snowflake key-pair auth** | Use Snowflake key-pair authentication instead of username/password. |
| **Restrict `.env` permissions** | On shared machines, restrict file access (e.g., `icacls` on Windows, `chmod 600` on Linux). |

---

## 3. Network & Access Control

| Layer | Current | Recommended |
|-------|---------|-------------|
| Airflow UI | `localhost:8080` (all interfaces) | Bind to `127.0.0.1` or use reverse proxy + SSO |
| Postgres | Internal Docker network only | ✅ Already isolated |
| S3 bucket | Public-ish? Verify bucket policy | **Private bucket + bucket policy** restricting to Snowflake's IAM role + Airflow execution role |
| Snowflake | Reachable from internet (standard) | Use network policy to restrict to allowed IPs/VPC |
| AWS credentials | Long-lived access keys | Use short-lived STS tokens / IAM roles |

### S3 Bucket Policy (recommended)
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<SNOWFLAKE_ACCOUNT_ID>:role/<SNOWFLAKE_STORAGE_ROLE>"
      },
      "Action": [
        "s3:GetObject",
        "s3:GetBucketLocation",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::claims-data-pipeline-2026-0808",
        "arn:aws:s3:::claims-data-pipeline-2026-0808/claims-data-pipeline/*"
      ]
    }
  ]
}
```

---

## 4. Data Protection

### 4.1 At rest
- **S3**: Enable **SSE-S3** (default) or **SSE-KMS** for server-side encryption.
- **Snowflake**: Snowflake encrypts all data at rest by default (Tri-Secret Secure / AES-256).
- **Postgres (Airflow metadata)**: Volume should be encrypted (Docker volume on encrypted disk).

### 4.2 In transit
- S3: HTTPS by default.
- Snowflake: TLS encrypted connections.
- Airflow UI/API: Use HTTPS via reverse proxy in production.

### 4.3 PHI / PII considerations
- The claims data contains patient IDs, amounts, and statuses — treat as sensitive.
- Consider **column-level masking** in Snowflake for downstream roles.
- Consider **row-level access policies** if multi-tenant.

### 4.4 Data retention
- Define and enforce a retention policy for S3 raw files and Snowflake tables.
- Use Snowflake **time travel / fail-safe** settings appropriately for the data class.

---

## 5. Audit & Observability

The pipeline already writes audit records to `PLATFORM_AUDIT_DB.AUDIT.PIPELINE_AUDIT`. That's excellent. Enhancements:

| Area | Recommendation |
|------|----------------|
| **Audit retention** | Keep audit data longer than source data (regulatory requirement). |
| **Alerting** | Add Airflow `email_on_failure` / `on_failure_callback` to notify the data team. |
| **Metrics** | Emit metrics (rows loaded, duration, status) to CloudWatch / Datadog / Prometheus. |
| **COPY history** | Periodically reconcile `PIPELINE_AUDIT` with Snowflake `COPY_HISTORY` for data integrity. |

---

## 6. DAG Code Best Practices

### 6.1 Critical fixes needed

| # | Issue | Severity |
|---|-------|----------|
| 1 | Invalid import: `from airflow.providers.standard.operators.python import PythonOperator` → should be `from airflow.operators.python import PythonOperator` | 🔴 Critical |
| 2 | No `default_args` → no retries, no timeout, no alerting | 🔴 Critical |
| 3 | `S3_BUCKET`, `S3_PREFIX`, `ENVIRONMENT` hardcoded | 🟠 High |

### 6.2 Recommended patterns

```python
# 1. Proper imports
from airflow.operators.python import PythonOperator

# 2. default_args
default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "email_on_failure": True,
    "email": ["data-ops@example.com"],
}

# 3. Centralized config via Airflow Variables
from airflow.models import Variable

S3_BUCKET = Variable.get("claims_s3_bucket", default_var="claims-data-pipeline-2026-0808")
S3_PREFIX = Variable.get("claims_s3_prefix", default_var="claims-data-pipeline/input/")
ENVIRONMENT = Variable.get("claims_environment", default_var="DEV")
```

### 6.3 Other DAG best practices
- Use **`logging`** instead of `print()`.
- Use **TaskFlow API** (`@task`) for cleaner code.
- Prefer native operators (`S3KeySensor`, `SnowflakeOperator`) where possible.
- Add `doc_md` to the DAG for UI documentation.
- Set `max_active_runs=1` to prevent concurrent pipeline runs.
- Validate `file_date` input with a strict regex before SQL construction (SQL injection hardening).

---

## 7. Docker / Deployment Security

### 7.1 Current docker-compose
- Image `apache/airflow:3.0.2` — pinned version ✅
- Postgres credentials are hardcoded (`airflow`/`airflow`) — ⚠️ fine for local dev, weak for shared environments.
- `.env` powers JWT + Fernet — ✅ good.

### 7.2 Recommendations
| Area | Recommendation |
|------|----------------|
| **Postgres credentials** | Use env vars / secrets for Postgres user/password. |
| **Resource limits** | Add `mem_limit` / `cpus` to each service. |
| **Image pinning** | Pin to exact digest (not just tag) for reproducibility. |
| **Healthchecks** | Add healthchecks for Airflow services (not just Postgres). |
| **Log rotation** | Configure Airflow log rotation to avoid unbounded log growth. |
| **Read-only root FS** | Consider `read_only: true` + tmpfs for containers (advanced). |

---

## 8. Input Validation

The CSV is loaded via Snowflake `COPY INTO` with `ON_ERROR = 'ABORT_STATEMENT'`. This is strict but consider:

| Check | Where | Description |
|-------|-------|-------------|
| File name regex | DAG | Validate `^claims_\d{8}\.csv$` before SQL |
| File size / row count | DAG / pre-check | Reject empty or absurdly large files |
| Schema validation | Pre-load | Use Snowflake file format + `VALIDATION_MODE` before actual load |
| Data quality | Post-load | Run row-count / null / range checks after COPY |

Example validation in DAG:
```python
import re

FILE_NAME_PATTERN = re.compile(r"^claims_\d{8}\.csv$")

if not FILE_NAME_PATTERN.match(file_name):
    raise ValueError(f"Unexpected file name format: {file_name}")
```

---

## 9. Production Hardening Checklist

- [ ] Fix PythonOperator import
- [ ] Add `default_args` (retries, timeout, alerting)
- [ ] Move config to Airflow Variables
- [ ] Use secrets backend (Vault / Secrets Manager)
- [ ] Use IAM roles instead of access keys
- [ ] Restrict S3 bucket policy
- [ ] Enable S3 encryption (SSE-KMS)
- [ ] Add Snowflake network policy
- [ ] Add Snowflake masking policies for PII columns
- [ ] Schedule via `schedule_interval` or `timetable` (currently manual)
- [ ] Add data-quality validation tasks
- [ ] Add SLA + alerting
- [ ] Enable Airflow log forwarding / central logging
- [ ] Add CI/CD (lint DAG, unit test, deploy)
- [ ] Pin container image digests
- [ ] Set Postgres credentials from secrets

---

## 10. Summary

The current pipeline is secure-by-default in several areas (encrypted Airflow connections, git-ignored `.env`, Snowflake encryption at rest), but needs hardening before production:

1. **Fix the broken import** (DAG won't load).
2. **Add retries/timeout/alerting**.
3. **Centralize configuration**.
4. **Move to secrets backend + IAM roles**.
5. **Add input validation + data quality checks**.