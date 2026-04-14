# =============================================================================
# Makefile - Common commands for local development and deployment
# =============================================================================

ifneq ($(filter-out help,$(MAKECMDGOALS)),)
$(warning DEPRECATED: Makefile targets are deprecated; use the ecomlake CLI instead.)
endif

.PHONY: help build up down restart logs shell log-task test lint format type-check clean clean-data version
.PHONY: dbt-deps dbt-build dbt-test local-demo local-demo-fast local-demo-full local-silver push-image build-versioned push-image-versioned
.PHONY: strict-mode easy-mode run-sample run-sample-strict run-sample-bq backfill-easy backfill-strict run-dims backfill-dims
.PHONY: run-dev-gcs dev-mode run-dev-docker prod-sim-mode run-prod-sim-docker

RUN_ID_OPT = $(if $(RUN_ID),--run-id $(RUN_ID),)
DEMO_DATE ?= 2020-01-05
DEMO_END_DATE ?= 2020-01-05
DEMO_LOOKBACK ?= 4
DEMO_DATES ?= 2020-01-01 2020-01-02 2020-01-03 2020-01-04 2020-01-05

# Default target
help:
	@echo "======================================================================"
	@echo "   E-commerce Data Pipeline - CLI & Make Commands"
	@echo "======================================================================"
	@echo ""
	@echo "Environment Control:"
	@echo "  make version         Show pipeline version information"
	@echo "  make up              Start local Airflow (Webserver: http://localhost:8080)"
	@echo "  make down            Stop Airflow services"
	@echo "  make restart         Restart Scheduler & Webserver"
	@echo "  make logs            Tail Scheduler logs"
	@echo "  make log-task        Tail a specific task log"
	@echo "      Required: DAG=<dag_id> RUN_ID=<run_id> TASK=<task_id>"
	@echo "      Optional: TRY=<n> (default: 1) LINES=<n> (default: 200)"
	@echo "  make shell           Open Bash in Scheduler container"
	@echo "  make clean           Destroy containers, images, volumes AND local data"
	@echo "  make clean-data      Wipe local 'data/silver' and 'data/metrics' only"
	@echo "  make strict-mode     Restart Airflow in PROD mode (Hard Validation Fails)"
	@echo "  make easy-mode       Restart Airflow in LOCAL mode (Soft Validation Fails)"
	@echo ""
	@echo "Pipeline Execution:"
	@echo "  make run-sample        Trigger Main Pipeline (Soft Mode)"
	@echo "      Required: DATE=YYYY-MM-DD"
	@echo "      Optional: RUN_ID=<custom_id>"
	@echo ""
	@echo "  make run-sample-strict Trigger Main Pipeline (Strict Mode)"
	@echo "      Required: DATE=YYYY-MM-DD"
	@echo ""
	@echo "  make run-sample-bq     Trigger Main Pipeline + BigQuery Load (Strict Mode)"
	@echo "      Required: DATE=YYYY-MM-DD"
	@echo "      Env Vars: GOOGLE_CLOUD_PROJECT, GCS_BUCKET must be set"
	@echo ""
	@echo "  make run-dims          Trigger Dimension Refresh Pipeline"
	@echo "      Required: DATE=YYYY-MM-DD"
	@echo ""
	@echo "  make backfill-easy     Backfill Main Pipeline (Soft Mode)"
	@echo "      Required: START=YYYY-MM-DD END=YYYY-MM-DD"
	@echo ""
	@echo "  make backfill-strict   Backfill Main Pipeline (Strict Mode)"
	@echo "      Required: START=YYYY-MM-DD END=YYYY-MM-DD"
	@echo ""
	@echo "  make run-dev-gcs       Run Pipeline with GCS (Native, No Docker)"
	@echo "  make run-sim-prod-gcs  Simulate prod run against GCS (Native, No Docker)"
	@echo "      Required: DATE=YYYY-MM-DD"
	@echo ""
	@echo "  make run-dev-docker    Run Pipeline with GCS (Docker + Airflow, dev)"
	@echo "  make run-prod-sim-docker Run Pipeline with GCS (Docker + Airflow, prod-sim)"
	@echo "      Required: DATE=YYYY-MM-DD"
	@echo "      Uses: gs://gcs-automation-project-raw (Bronze)"
	@echo "            gs://gcs-automation-project-silver (Silver)"
	@echo ""
	@echo "Development & Testing:"
	@echo "  make test            Run Unit Tests (pytest)"
	@echo "  make lint            Run Linter (ruff)"
	@echo "  make format          Auto-format Code (black + isort)"
	@echo "  make type-check      Run Type Checker (mypy)"
	@echo "  make local-silver    Run dbt + Validation locally (No Docker)"
	@echo "  make local-silver-strict Run dbt + Validation locally (Strict)"
	@echo "  make local-enriched  Run enriched transforms locally (optional: DATE=YYYY-MM-DD)"
	@echo "  make local-enriched-strict Run enriched transforms locally (Strict, optional: DATE=YYYY-MM-DD)"
	@echo "  make local-dims      Run customer + product catalog dims locally"
	@echo "  make local-dims-strict Run customer + product catalog dims locally (Strict)"
	@echo "  make local-demo      Run local E2E demo (dims 3-day + silver 3-day + enriched)"
	@echo "  make local-demo-fast Run fast E2E demo (pre-cooked dims + silver + enriched)"
	@echo "  make local-demo-full Run full E2E demo (alias for local-demo)"
	@echo ""
	@echo "dbt Utilities:"
	@echo "  make dbt-deps        Install dbt packages"
	@echo "  make dbt-build       Build all Base Silver models"
	@echo "  make dbt-test        Run dbt data tests"
	@echo ""
	@echo "Deployment:"
	@echo "  make build-versioned       Build Docker image with Git version tags"
	@echo "  make push-image            Build & Push Docker Image (latest only)"
	@echo "      Required: PROJECT_ID=<gcp_project_id>"
	@echo "  make push-image-versioned  Build & Push versioned Docker Image"
	@echo "      Required: PROJECT_ID=<gcp_project_id>"
	@echo "      Tags: <VERSION> (git tag or branch-commit), latest"
	@echo ""


# ==============================================================================
# Docker Commands
# ==============================================================================

version:
	@python scripts/version.py

build:
	@echo "Building Docker image (building scheduler service only, others will reuse)..."
	docker-compose build airflow-scheduler

up:
	@echo "Starting Airflow..."
	@echo "Webserver will be available at http://localhost:8080"
	@echo "Username: airflow | Password: airflow"
	docker-compose up airflow-init
	docker-compose up -d

down:
	docker-compose down

restart:
	docker-compose restart airflow-scheduler airflow-webserver

logs:
	docker-compose logs -f airflow-scheduler

log-task:
ifndef DAG
	@echo "ERROR: DAG not set. Usage: make log-task DAG=<dag_id> RUN_ID=<run_id> TASK=<task_id> [TRY=N] [LINES=N]"
	@exit 1
endif
ifndef RUN_ID
	@echo "ERROR: RUN_ID not set. Usage: make log-task DAG=<dag_id> RUN_ID=<run_id> TASK=<task_id> [TRY=N] [LINES=N]"
	@exit 1
endif
ifndef TASK
	@echo "ERROR: TASK not set. Usage: make log-task DAG=<dag_id> RUN_ID=<run_id> TASK=<task_id> [TRY=N] [LINES=N]"
	@exit 1
endif
	@LINES=$${LINES:-200}; TRY=$${TRY:-1}; \
	docker-compose exec airflow-scheduler tail -n $$LINES \
	"/opt/airflow/logs/dag_id=$${DAG}/run_id=$${RUN_ID}/task_id=$${TASK}/attempt=$${TRY}.log"

shell:
	docker-compose exec airflow-scheduler bash

clean-data:
	@echo "Cleaning local data directories..."
	rm -rf data/silver/base/*
	rm -rf data/silver/dims/*
	rm -rf data/silver/enriched/*
	rm -rf data/metrics/*
	@echo "Local data cleared!"

clean:
	@echo "WARNING: This will remove all containers, volumes, custom image, AND local data!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		docker-compose down -v; \
		docker rmi ecom-datalake-pipeline:latest 2>/dev/null || true; \
		$(MAKE) clean-data; \
		echo "Cleanup complete!"; \
	fi

# ==============================================================================
# Pipeline Runs
# ==============================================================================

strict-mode:
	PIPELINE_ENV=prod \
	OBSERVABILITY_ENV=local \
	BRONZE_QA_FAIL=true \
	BQ_LOAD_ENABLED=false \
	GOLD_PIPELINE_ENABLED=false \
	docker-compose up -d --force-recreate airflow-scheduler airflow-webserver

easy-mode:
	PIPELINE_ENV=local \
	BRONZE_QA_FAIL=false \
	BQ_LOAD_ENABLED=false \
	GOLD_PIPELINE_ENABLED=false \
	docker-compose up -d --force-recreate airflow-scheduler airflow-webserver

run-sample:
ifndef DATE
	@echo "ERROR: DATE not set. Usage: make run-sample DATE=YYYY-MM-DD"
	@exit 1
endif
	docker-compose exec airflow-scheduler \
		airflow dags trigger ecom_silver_to_gold_pipeline --exec-date $(DATE) $(RUN_ID_OPT)

run-sample-strict: strict-mode run-sample

run-sample-bq:
ifndef DATE
	@echo "ERROR: DATE not set. Usage: make run-sample-bq DATE=YYYY-MM-DD"
	@exit 1
endif
ifndef GOOGLE_CLOUD_PROJECT
	@echo "ERROR: GOOGLE_CLOUD_PROJECT not set"
	@exit 1
endif
ifndef GCS_BUCKET
	@echo "ERROR: GCS_BUCKET not set"
	@exit 1
endif
	PIPELINE_ENV=prod \
	OBSERVABILITY_ENV=local \
	BRONZE_QA_FAIL=true \
	BQ_LOAD_ENABLED=true \
	GOLD_PIPELINE_ENABLED=false \
	docker-compose up -d --force-recreate airflow-scheduler airflow-webserver
	docker-compose exec airflow-scheduler \
		airflow dags trigger ecom_silver_to_gold_pipeline --exec-date $(DATE)

run-dims:
ifndef DATE
	@echo "ERROR: DATE not set. Usage: make run-dims DATE=YYYY-MM-DD"
	@exit 1
endif
	docker-compose exec airflow-scheduler \
		airflow dags trigger ecom_dim_refresh_pipeline --exec-date $(DATE) $(RUN_ID_OPT)

backfill-dims:
ifndef START
	@echo "ERROR: START not set. Usage: make backfill-dims START=YYYY-MM-DD END=YYYY-MM-DD"
	@exit 1
endif
ifndef END
	@echo "ERROR: END not set. Usage: make backfill-dims START=YYYY-MM-DD END=YYYY-MM-DD"
	@exit 1
endif
	docker-compose exec airflow-scheduler \
		airflow dags backfill ecom_dim_refresh_pipeline -s $(START) -e $(END)

backfill-easy: easy-mode
ifndef START
	@echo "ERROR: START not set. Usage: make backfill-easy START=YYYY-MM-DD END=YYYY-MM-DD"
	@exit 1
endif
ifndef END
	@echo "ERROR: END not set. Usage: make backfill-easy START=YYYY-MM-DD END=YYYY-MM-DD"
	@exit 1
endif
	docker-compose exec airflow-scheduler \
		airflow dags backfill ecom_silver_to_gold_pipeline -s $(START) -e $(END)

backfill-strict: strict-mode
ifndef START
	@echo "ERROR: START not set. Usage: make backfill-strict START=YYYY-MM-DD END=YYYY-MM-DD"
	@exit 1
endif
ifndef END
	@echo "ERROR: END not set. Usage: make backfill-strict START=YYYY-MM-DD END=YYYY-MM-DD"
	@exit 1
endif
	docker-compose exec airflow-scheduler \
		airflow dags backfill ecom_silver_to_gold_pipeline -s $(START) -e $(END)

# Run pipeline in dev mode with GCS buckets (No Docker, No BQ)
run-dev-gcs:
ifndef DATE
	@echo "ERROR: DATE not set. Usage: make run-dev-gcs DATE=2025-10-04"
	@exit 1
endif
	@echo "=========================================="
	@echo "Running Dev Pipeline with GCS (Native)"
	@echo "Date: $(DATE)"
	@echo "Bronze: gs://gcs-automation-project-raw/data/bronze"
	@echo "Silver: gs://gcs-automation-project-silver/data/silver"
	@echo "=========================================="
	@./scripts/run_dev_pipeline.sh $(DATE)

# Simulated prod run against GCS (native, no Docker)
run-sim-prod-gcs:
ifndef DATE
	@echo "ERROR: DATE not set. Usage: make run-sim-prod-gcs DATE=2025-10-04"
	@exit 1
endif
	@echo "=========================================="
	@echo "Running Simulated Prod Pipeline (GCS)"
	@echo "Date: $(DATE)"
	@echo "Bronze: gs://gcs-automation-project-raw/ecom/raw"
	@echo "Silver: gs://gcs-automation-project-silver/data/silver/base"
	@echo "Enriched: gs://gcs-automation-project-silver/data/silver/enriched"
	@echo "=========================================="
	@./scripts/run_sim_prod_gcs.sh $(DATE)

# Start Airflow in dev mode (GCS buckets, no BQ load)
dev-mode:
	@echo "Starting Airflow in DEV mode..."
	@echo "  - Environment: dev"
	@echo "  - Bronze: gs://gcs-automation-project-raw"
	@echo "  - Silver: gs://gcs-automation-project-silver"
	@echo "  - BigQuery: DISABLED"
	@echo "  - Gold: DISABLED"
	PIPELINE_ENV=dev \
	OBSERVABILITY_ENV=dev \
	BQ_LOAD_ENABLED=false \
	GOLD_PIPELINE_ENABLED=false \
	docker compose --env-file docker.env up -d --force-recreate airflow-scheduler airflow-webserver
	@echo ""
	@echo "Airflow UI: http://localhost:8080"
	@echo "Username: airflow | Password: airflow"

# Run pipeline in Docker with GCS buckets (dev defaults)
run-dev-docker: dev-mode
ifndef DATE
	@echo "ERROR: DATE not set. Usage: make run-dev-docker DATE=2025-10-04"
	@exit 1
endif
	@echo "=========================================="
	@echo "Triggering DAG in Airflow (Docker)"
	@echo "Date: $(DATE)"
	@echo "=========================================="
	@sleep 5  # Give Airflow time to start
	docker-compose exec airflow-scheduler \
		airflow dags trigger ecom_silver_to_gold_pipeline --exec-date $(DATE)
	@echo ""
	@echo "DAG triggered! Monitor at http://localhost:8080"

# Start Airflow in prod-sim mode (GCS buckets, no BQ load)
prod-sim-mode:
	@echo "Starting Airflow in PROD-SIM mode..."
	@echo "  - Environment: prod"
	@echo "  - Bronze: gs://gcs-automation-project-raw"
	@echo "  - Silver: gs://gcs-automation-project-silver"
	@echo "  - BigQuery: DISABLED"
	@echo "  - Gold: DISABLED"
	PIPELINE_ENV=prod \
	OBSERVABILITY_ENV=prod \
	BQ_LOAD_ENABLED=false \
	GOLD_PIPELINE_ENABLED=false \
	docker compose --env-file docker.env up -d --force-recreate airflow-scheduler airflow-webserver
	@echo ""
	@echo "Airflow UI: http://localhost:8080"
	@echo "Username: airflow | Password: airflow"

# Run pipeline in Docker with GCS buckets (prod-sim defaults)
run-prod-sim-docker: prod-sim-mode
ifndef DATE
	@echo "ERROR: DATE not set. Usage: make run-prod-sim-docker DATE=2025-10-04"
	@exit 1
endif
	@echo "=========================================="
	@echo "Triggering DAG in Airflow (Docker, PROD-SIM)"
	@echo "Date: $(DATE)"
	@echo "=========================================="
	@sleep 5  # Give Airflow time to start
	docker-compose exec airflow-scheduler \
		airflow dags trigger ecom_silver_to_gold_pipeline --exec-date $(DATE)
	@echo ""
	@echo "DAG triggered! Monitor at http://localhost:8080"

# ==============================================================================
# Testing & Code Quality
# ==============================================================================

test:
	pytest tests/unit/ -v

lint:
	ruff check src/ tests/ airflow/
	yamllint .

format:
	black src/ tests/ airflow/
	isort src/ tests/ airflow/

type-check:
	mypy src/

# ==============================================================================
# Docs & Profiling
# ==============================================================================

PROFILE_DATE_RANGE ?= 2025-10-01..2025-10-01

profile-bronze-samples:
	python scripts/describe_parquet_samples.py \
		--date-range $(PROFILE_DATE_RANGE) \
		--output docs/data/BRONZE_PROFILE_REPORT.md \
		--schema-json docs/data/BRONZE_SCHEMA_MAP.json \
		--update-contract docs/data/DATA_CONTRACT.md \
		--data-dictionary docs/data/DATA_DICTIONARY.md

# ==============================================================================
# dbt Commands
# ==============================================================================

dbt-deps:
	cd dbt_duckdb && dbt deps --project-dir . --profiles-dir .

dbt-build:
	cd dbt_duckdb && dbt build --project-dir . --profiles-dir .

dbt-test:
	cd dbt_duckdb && dbt test --project-dir . --profiles-dir .

# Build Silver locally (no Docker, for quick testing)
local-demo:
	@echo "Running local E2E demo (dims 3-day + silver 3-day + enriched day 3)..."
	@$(MAKE) dbt-deps
	for day in $(DEMO_DATES); do \
		PIPELINE_ENV=local \
			BRONZE_BASE_PATH="$(PWD)/samples/bronze" \
			SILVER_BASE_PATH="$(PWD)/data/silver/base" \
			SILVER_DIMS_PATH="$(PWD)/data/silver/dims" \
			LOGS_BASE_PATH="/tmp/ecom_logs" \
			METRICS_BASE_PATH="/tmp/ecom_metrics" \
			python scripts/run_dims_from_spec.py --run-date "$$day"; \
		LOGS_BASE_PATH="/tmp/ecom_logs" \
			METRICS_BASE_PATH="/tmp/ecom_metrics" \
			python -m src.validation.dims_snapshot --run-date "$$day" --run-id "local_demo_$$day"; \
	done
	PIPELINE_ENV=local \
		BRONZE_BASE_PATH="$(PWD)/samples/bronze" \
		SILVER_BASE_PATH="$(PWD)/data/silver/base" \
		SILVER_DIMS_PATH="$(PWD)/data/silver/dims" \
		LOGS_BASE_PATH="/tmp/ecom_logs" \
		METRICS_BASE_PATH="/tmp/ecom_metrics" \
		python -m src.runners.base_silver --select "base_silver.*" \
		--vars "{run_date: '$(DEMO_END_DATE)', lookback_days: $(DEMO_LOOKBACK)}"
	LOGS_BASE_PATH="/tmp/ecom_logs" \
		METRICS_BASE_PATH="/tmp/ecom_metrics" \
		python -m src.validation.silver \
		--bronze-path samples/bronze \
		--silver-path data/silver/base \
		--quarantine-path data/silver/base/quarantine \
		--partition-date $(DEMO_END_DATE) \
		--lookback-days $(DEMO_LOOKBACK) \
		--tables orders,order_items,shopping_carts,cart_items,returns,return_items \
		--output-report docs/validation_reports/SILVER_QUALITY_FULL.md
	@$(MAKE) dbt-test
	@$(MAKE) local-enriched DATE=$(DEMO_END_DATE)

local-demo-fast:
	@echo "Running fast local demo (5-day complete pipeline)..."
	@echo "Sample: 2020-01-01 through 2020-01-05 with complete customer history"
	@$(MAKE) dbt-deps
	@echo "Generating dims snapshots from Bronze data..."
	@mkdir -p data/silver/dims
	@$(MAKE) local-dims DATE=$(DEMO_DATE)
	@echo "✓ Dims generated (customers + product_catalog for $(DEMO_DATE))"
	@echo "Running single-day pipeline for $(DEMO_DATE)..."
	PIPELINE_ENV=local \
		BRONZE_BASE_PATH="$(PWD)/samples/bronze" \
		SILVER_BASE_PATH="$(PWD)/data/silver/base" \
		SILVER_DIMS_PATH="$(PWD)/data/silver/dims" \
		DBT_LOG_PATH="/tmp/dbt_logs" \
		DBT_TARGET_PATH="/tmp/dbt_target" \
		LOGS_BASE_PATH="/tmp/ecom_logs" \
		METRICS_BASE_PATH="/tmp/ecom_metrics" \
		python -m src.runners.base_silver --select "base_silver.*" \
		--vars "{run_date: '$(DEMO_DATE)', lookback_days: 0}"
	LOGS_BASE_PATH="/tmp/ecom_logs" \
		METRICS_BASE_PATH="/tmp/ecom_metrics" \
		python -m src.validation.silver \
		--bronze-path samples/bronze \
		--silver-path data/silver/base \
		--quarantine-path data/silver/base/quarantine \
		--partition-date $(DEMO_DATE) \
		--lookback-days 0 \
		--tables orders,order_items,shopping_carts,cart_items,returns,return_items \
		--output-report docs/validation_reports/SILVER_QUALITY_FULL.md
	@$(MAKE) dbt-test
	@$(MAKE) local-enriched DATE=$(DEMO_DATE)
	@echo "✅ Demo complete!"

local-demo-full:
	@echo "Running full local demo (alias for local-demo)..."
	@$(MAKE) local-demo

local-silver:
	PIPELINE_ENV=local \
		BRONZE_BASE_PATH="$(PWD)/samples/bronze" \
		SILVER_BASE_PATH="$(PWD)/data/silver/base" \
		SILVER_DIMS_PATH="$(PWD)/data/silver/dims" \
		python -m src.runners.base_silver --select "base_silver.*" \
		$(if $(DATE),--vars '{"run_date": "$(DATE)"$(COMMA) "lookback_days": 0}',) \
		$(if $(DATE),--exclude stg_ecommerce__customers stg_ecommerce__customers_quarantine stg_ecommerce__product_catalog stg_ecommerce__product_catalog_quarantine,)
	python -m src.validation.silver \
		--bronze-path samples/bronze \
		--silver-path data/silver/base \
		--quarantine-path data/silver/base/quarantine \
		--output-report docs/validation_reports/SILVER_QUALITY_FULL.md \
		$(if $(DATE),--partition-date $(DATE) --tables orders,order_items,shopping_carts,cart_items,returns,return_items,)

local-silver-strict:
	PIPELINE_ENV=local \
		BRONZE_BASE_PATH="$(PWD)/samples/bronze" \
		SILVER_BASE_PATH="$(PWD)/data/silver/base" \
		SILVER_DIMS_PATH="$(PWD)/data/silver/dims" \
		python -m src.runners.base_silver --select "base_silver.*" \
		$(if $(DATE),--vars '{"run_date": "$(DATE)"$(COMMA) "lookback_days": 0}',) \
		$(if $(DATE),--exclude stg_ecommerce__customers stg_ecommerce__customers_quarantine stg_ecommerce__product_catalog stg_ecommerce__product_catalog_quarantine,)
	python -m src.validation.silver \
		--bronze-path samples/bronze \
		--silver-path data/silver/base \
		--quarantine-path data/silver/base/quarantine \
		--output-report docs/validation_reports/SILVER_QUALITY_FULL.md \
		$(if $(DATE),--partition-date $(DATE) --tables orders,order_items,shopping_carts,cart_items,returns,return_items,) \
		--enforce-quality

local-enriched:
	PIPELINE_ENV=local \
		BRONZE_BASE_PATH="$(PWD)/samples/bronze" \
		SILVER_BASE_PATH="$(PWD)/data/silver/base" \
		SILVER_DIMS_PATH="$(PWD)/data/silver/dims" \
		SILVER_ENRICHED_PATH="$(PWD)/data/silver/enriched" \
		LOGS_BASE_PATH="/tmp/ecom_logs" \
		METRICS_BASE_PATH="/tmp/ecom_metrics" \
		python scripts/run_enriched_all_samples.py \
		--base-path "$(PWD)/data/silver/base" \
		--output-path "$(PWD)/data/silver/enriched" \
		--per-date \
		$(if $(DATE),--ingest-dt $(DATE),)

local-enriched-strict:
	PIPELINE_ENV=local \
		BRONZE_BASE_PATH="$(PWD)/samples/bronze" \
		SILVER_BASE_PATH="$(PWD)/data/silver/base" \
		SILVER_DIMS_PATH="$(PWD)/data/silver/dims" \
		SILVER_ENRICHED_PATH="$(PWD)/data/silver/enriched" \
		python scripts/run_enriched_all_samples.py \
		--base-path "$(PWD)/data/silver/base" \
		--output-path "$(PWD)/data/silver/enriched" \
		--per-date \
		--enforce-quality \
		$(if $(DATE),--ingest-dt $(DATE),)

local-dims:
	PIPELINE_ENV=local \
		BRONZE_BASE_PATH="$(PWD)/samples/bronze" \
		SILVER_BASE_PATH="$(PWD)/data/silver/base" \
		SILVER_DIMS_PATH="$(PWD)/data/silver/dims" \
		LOGS_BASE_PATH="/tmp/ecom_logs" \
		METRICS_BASE_PATH="/tmp/ecom_metrics" \
		python scripts/run_dims_from_spec.py \
		$(if $(DATE),--run-date $(DATE),)
	LOGS_BASE_PATH="/tmp/ecom_logs" \
		METRICS_BASE_PATH="/tmp/ecom_metrics" \
		python -m src.validation.silver \
		--bronze-path samples/bronze \
		--silver-path data/silver/base \
		--quarantine-path data/silver/base/quarantine \
		--output-report docs/validation_reports/SILVER_QUALITY.md \
		--tables customers,product_catalog

local-dims-strict:
	PIPELINE_ENV=local \
		BRONZE_BASE_PATH="$(PWD)/samples/bronze" \
		SILVER_BASE_PATH="$(PWD)/data/silver/base" \
		SILVER_DIMS_PATH="$(PWD)/data/silver/dims" \
		python scripts/run_dims_from_spec.py \
		$(if $(DATE),--run-date $(DATE),)
	python -m src.validation.silver \
		--bronze-path samples/bronze \
		--silver-path data/silver/base \
		--quarantine-path data/silver/base/quarantine \
		--output-report docs/validation_reports/SILVER_QUALITY.md \
		--tables customers,product_catalog \
		--enforce-quality

# ==============================================================================
# Production Deployment (requires gcloud CLI + PROJECT_ID env var)
# ==============================================================================

# Git version variables
GIT_COMMIT := $(shell git rev-parse --short HEAD)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GIT_TAG := $(shell git describe --tags --exact-match 2>/dev/null || echo "")
VERSION := $(if $(GIT_TAG),$(GIT_TAG),$(GIT_BRANCH)-$(GIT_COMMIT))
IMAGE_NAME := ecom-datalake-pipeline
COMMA := ,

build-versioned:
	@echo "Building versioned Docker image..."
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Git Branch: $(GIT_BRANCH)"
	@echo "Version: $(VERSION)"
	docker build \
		--build-arg GIT_COMMIT=$(GIT_COMMIT) \
		--build-arg GIT_BRANCH=$(GIT_BRANCH) \
		--build-arg VERSION=$(VERSION) \
		-t $(IMAGE_NAME):$(VERSION) \
		-t $(IMAGE_NAME):latest \
		.
	@echo "Built images:"
	@echo "  - $(IMAGE_NAME):$(VERSION)"
	@echo "  - $(IMAGE_NAME):latest"

push-image:
ifndef PROJECT_ID
	@echo "ERROR: PROJECT_ID not set. Usage: make push-image PROJECT_ID=my-project-123"
	@exit 1
endif
	@echo "Building and pushing image to Artifact Registry..."
	docker build -t ecom-datalake-pipeline:latest .
	docker tag ecom-datalake-pipeline:latest \
		us-central1-docker.pkg.dev/$(PROJECT_ID)/airflow-images/ecom-datalake-pipeline:latest
	gcloud auth configure-docker us-central1-docker.pkg.dev
	docker push us-central1-docker.pkg.dev/$(PROJECT_ID)/airflow-images/ecom-datalake-pipeline:latest

push-image-versioned:
ifndef PROJECT_ID
	@echo "ERROR: PROJECT_ID not set. Usage: make push-image-versioned PROJECT_ID=my-project-123"
	@exit 1
endif
	@echo "Building and pushing versioned image to Artifact Registry..."
	@echo "Version: $(VERSION)"
	@$(MAKE) build-versioned
	docker tag $(IMAGE_NAME):$(VERSION) \
		us-central1-docker.pkg.dev/$(PROJECT_ID)/airflow-images/$(IMAGE_NAME):$(VERSION)
	docker tag $(IMAGE_NAME):$(VERSION) \
		us-central1-docker.pkg.dev/$(PROJECT_ID)/airflow-images/$(IMAGE_NAME):latest
	gcloud auth configure-docker us-central1-docker.pkg.dev
	docker push us-central1-docker.pkg.dev/$(PROJECT_ID)/airflow-images/$(IMAGE_NAME):$(VERSION)
	docker push us-central1-docker.pkg.dev/$(PROJECT_ID)/airflow-images/$(IMAGE_NAME):latest
	@echo "Pushed images:"
	@echo "  - us-central1-docker.pkg.dev/$(PROJECT_ID)/airflow-images/$(IMAGE_NAME):$(VERSION)"
	@echo "  - us-central1-docker.pkg.dev/$(PROJECT_ID)/airflow-images/$(IMAGE_NAME):latest"
	@echo "Image pushed successfully!"
