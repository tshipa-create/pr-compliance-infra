# PR compliance pipeline - developer and reviewer entry points.
# Requires: docker (compose v2), git, bash. Windows: use Git Bash or WSL.

SHELL := /bin/bash
COMPOSE := docker compose
DC_EXEC := $(COMPOSE) exec -T airflow-scheduler
envval = $(or $(shell grep -E '^$(1)=' .env 2>/dev/null | cut -d= -f2),$(2))
AIRFLOW_PORT := $(call envval,AIRFLOW_PORT,8080)
MAILPIT_PORT := $(call envval,MAILPIT_PORT,8025)
GRAFANA_PORT := $(call envval,GRAFANA_PORT,3000)
DB_PORT := $(call envval,COMPLIANCE_DB_PORT,5433)
DEMO_REPOS ?= cli,version,operating-system,supervisor
DEMO_DAYS ?= 7

.PHONY: help check up down clean logs ps trigger demo test-alert test test-dag psql urls

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-11s\033[0m %s\n", $$1, $$2}'

check: ## Fail fast if the submodule or .env is missing
	@test -f processing/pyproject.toml || { echo "processing/ is empty: clone with --recursive or run 'git submodule update --init'"; exit 1; }
	@test -f .env || { echo ".env missing: cp .env.example .env and set GITHUB_TOKEN"; exit 1; }
	@grep -qE '^GITHUB_TOKEN=.+' .env || { echo "GITHUB_TOKEN is empty in .env"; exit 1; }

up: check ## Build and start the stack, wait until healthy
	mkdir -p data logs
	$(COMPOSE) up -d --build --wait
	@$(MAKE) --no-print-directory urls

down: ## Stop the stack (keeps volumes and data)
	$(COMPOSE) down

clean: ## Stop the stack and delete volumes, ./data and ./logs
	$(COMPOSE) down -v
	rm -rf ./data ./logs

logs: ## Follow scheduler logs
	$(COMPOSE) logs -f airflow-scheduler

ps: ## Service health
	$(COMPOSE) ps

trigger: ## Trigger a manual run (processes the previous full UTC day)
	$(DC_EXEC) airflow dags trigger pr_compliance

demo: check ## Small end-to-end run: DEMO_REPOS over the last DEMO_DAYS days
	@echo "Demo: repos=$(DEMO_REPOS) lookback=$(DEMO_DAYS)d"
	REPO_ALLOWLIST=$(DEMO_REPOS) LOOKBACK_DAYS=$(DEMO_DAYS) DQ_MIN_PRS=0 $(COMPOSE) up -d --wait
	$(DC_EXEC) airflow dags trigger pr_compliance
	@echo "Watch it at http://localhost:$(AIRFLOW_PORT)/dags/pr_compliance (airflow / airflow)"
	@echo "Back to the full configuration afterwards: make up"

test-alert: ## Send a test failure alert to the Mailpit inbox
	$(DC_EXEC) python -c "from types import SimpleNamespace as N; from pr_compliance.alerts import notify_failure; \
	notify_failure({'ti': N(dag_id='pr_compliance', task_id='test_alert', map_index=-1, try_number=1, run_id='manual__test-alert', log_url=None), 'exception': RuntimeError('test alert from make test-alert')})"
	@echo "Open http://localhost:$(MAILPIT_PORT)"

test-dag: ## DAG integrity + unit tests inside the Airflow image
	$(COMPOSE) run --rm -T -v "$(CURDIR)/processing:/opt/src" airflow-cli bash -c \
	  "uv pip install -q pytest responses freezegun && cd /opt/src && python -m pytest tests/test_dag.py tests/unit -q -p no:cacheprovider"

test: ## Processing test suite on the host (needs uv)
	cd processing && uv run pytest -m "not integration"

psql: ## Open psql on the output database
	$(COMPOSE) exec compliance-db sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

urls: ## Print the local URLs
	@echo "Airflow   http://localhost:$(AIRFLOW_PORT)  (airflow / airflow)"
	@echo "Mailpit   http://localhost:$(MAILPIT_PORT)"
	@echo "Grafana   http://localhost:$(GRAFANA_PORT)"
	@echo "Postgres  localhost:$(DB_PORT)  db=compliance"
