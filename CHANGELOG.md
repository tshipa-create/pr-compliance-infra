# Changelog

## v1.0.0 — 2026-11-24

- Docker Compose stack: Airflow 3.3.0 (LocalExecutor), metadata and output Postgres 16,
  Mailpit, Grafana 12 with a provisioned dashboard.
- Image installs `pr-compliance-processing` v1.0.0 under Airflow's constraints-3.13.
- Makefile (`up`, `demo`, `test-alert`, `test-dag`, …), configurable ports.
- CI: compose/Dockerfile/YAML validation, Trivy scan, end-to-end smoke test.
