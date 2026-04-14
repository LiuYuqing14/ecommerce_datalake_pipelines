# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.7] - 2026-02-01 - BQ Load & Gold Marts Reliability

### Added

- **Gold dataset override**: `BQ_GOLD_DATASET` wiring for dbt gold marts output schema
- **Secrets guidance**: Fernet key and secrets backend notes in env strategy + runbook
- **Env examples**: Expanded `.env.example` and `.envrc` placeholders for secrets and datasets

### Fixed

- **BQ partition types**: Preserve date partition column types in enriched outputs to satisfy BigQuery partitioning
- **Gold marts SQL**: Corrected gold model wiring to avoid invalid compiled SQL in baked images

### Changed

- **Airflow env**: Pass `BQ_GOLD_DATASET` into DAG task environments

## [1.0.6] - 2026-01-28 - Staging Promotion & Validation

### Added

- **Staging promotion workflow**: Implemented Silver-to-Gold promotion logic with support for `_staging/` publish paths
- **Integrated validation gates**: Promotion flow now enforces validation checks before publishing to staging
- **Cart items manifests**: Added manifest support for `cart_items` in main pipelines and sample datasets

### Fixed

- **dbt dependency management**: Added automated cleanup and lock-file consistency checks for `dbt deps`
- **Validation logging**: Corrected log formatting and updated unit tests for staging publish paths
- **Staging tests**: Resolved test suite failures related to promotion flow logic

### Changed

- **Docs**: Major documentation refresh across `ARCHITECTURE.md`, `DEPLOYMENT_GUIDE.md`, and `VALIDATION_GUIDE.md`
- **Environment config**: Updated `docker.env` examples and `run_sim_prod_gcs.sh` to support staging environment variables
- **Pipeline settings**: Updated `config/config.yml` with default staging promotion parameters

## [1.0.5] - 2026-01-27 - Validation & Backfill Gates

### Added

- **Dims backfill flag**: `DIMS_CUSTOMERS_IGNORE_SIGNUP_DATE` to include all customers for backfills
- **FK coverage helper**: `scripts/adhoc/check_customer_fk_coverage.py`

### Changed

- **Enriched validation order**: Validate enriched outputs after syncing to GCS
- **Docs**: Updated pipeline flow and validation guidance for post-sync checks

## [1.0.4] - 2026-01-27 - Reliability & Packaging Patch

### Fixed

- **CLI packaging**: Install now exposes the `src` module so `ecomlake` runs after `pip install -e .`
- **Local demo**: dbt log/target paths consistently set to `/tmp`; demo-full defaults wired correctly
- **Airflow runs**: `start_date` aligned to the earliest dataset date to prevent empty manual runs
- **Parquet fallback**: Local fallback reads no longer mis-route to GCS paths
- **Manifest inventory**: Base Silver `_MANIFEST.json` now includes file listings used by fast validation paths
- **Mypy checks**: Resolved typing issues in validation tests

### Changed

- **Docs**: Version metadata and changelog link surfaced in README

## [1.0.3] - 2026-01-26 - CLI Suite Release

### Added

- **Click-based CLI**: `ecomlake` command with organized action-style groups
- **Command coverage**: Bronze, Silver, Enriched, dims, pipeline, Airflow, dbt, dev, deploy, and local workflows
- **CLI help**: Expanded, example-rich help output for top-level and groups

### Changed

- **Docs**: Updated README and CLI usage guides to be ecomlake-first
- **Config precedence**: `ECOM_CONFIG_PATH` supported as default config override

### Deprecated

- **Legacy entrypoints**: Makefile targets and direct script/module entrypoints now emit deprecation warnings

### Fixed

- **Test coverage**: Added targeted unit tests to restore coverage above 60%

## [1.0.2] - 2026-01-25 - Honest Sample Data

### Fixed

- **Bronze sample completeness**: Replaced incomplete scattered-date sample with complete 5-day sample (2020-01-01 through 2020-01-05)
- **Dims snapshot validation**: CI now validates actual dims generation logic instead of using pre-cooked snapshots
- **Sample includes**:
  - Complete customer history (all signups from 2019-01-01 through 2020-01-05 = 370 partitions)
  - Complete product catalog (all 5 categories)
  - 5 consecutive days of fact tables: orders, order_items, shopping_carts, **cart_items**, returns, return_items

### Removed

- **Pre-cooked dims workaround**: Deleted `samples/dims_samples.zip` as dims now generate correctly from complete Bronze data

### Changed

- **CI workflow**: Updated to process 2020-01-01 through 2020-01-05 (5-day window)
- **Local demo**: Now generates dims from Bronze sample data instead of using pre-cooked snapshots
- **Demo date**: Changed from 2023-01-01/2024-01-03 to 2020-01-05 to align with complete sample data
- **Sample size**: Increased from 9.1MB to 16MB to include complete customer history and cart_items data

### Validation

- CI validates complete pipeline: Bronze → Dims → Silver → Enriched → Gold
- All 10 enriched tables produce data with honest dims snapshot generation

## [1.0.0] - 2026-01-23 - Feature Complete Release

### Summary

Production-ready medallion lakehouse pipeline with Bronze → Silver → Gold transformations. Full observability, three-layer validation framework, spec-driven orchestration, and 51% test coverage.

### Added

- **Spec-Driven Orchestration**: Layered YAML specs (`config/specs/*.yml`) drive table lists, partitions, and validation gates
- **Dimension Snapshot Pattern**: Daily dimension snapshots with freshness gates and `_latest.json` pointers
- **Three-Layer Validation Framework**: Modular validation packages for Bronze, Silver, and Enriched layers
- **Staging Prefix Pattern**: Atomic GCS publishes via `_staging/<run_id>/` with manifests and versioned run folders
- **Local BQ Load Testing**: Mock BigQuery load validation for parquet schema/format verification
- **Full E2E CI Pipeline**: Automated Bronze→Silver→Enriched→Gold pipeline check on sample data
- **GCS Validation Reports**: Production validation reports published to GCS for stakeholder visibility
- **Partition Lookback**: `lookback_days` parameter for Bronze/Silver validation to handle incremental processing

### Changed

- **Replaced gsutil with gcloud storage**: Modern GCS tooling with atomic sync operations
- **Refactored validation modules**: Broke down 500+ line monoliths into modular packages with shared utilities
- **Enhanced Airflow DAGs**: Two-DAG architecture (`ecom_dim_refresh_pipeline`, `ecom_silver_to_gold_pipeline`) with dynamic task generation from specs
- **Improved Docker compatibility**: Redirect dbt target/log paths to `/tmp` to fix macOS VirtioFS file locking (Errno 35)
- **Optimized GCS operations**: Smart path resolution with local fallbacks for dev environment

### Fixed

- **Schema consistency**: All base_silver models now consistently generate `ingestion_dt` metadata field
- **Product catalog partitioning**: Snapshot-based partitioning for dimension tables (not event-based)
- **Enriched parquet writes**: Strict schema handling with proper ingestion_dt metadata
- **Test coverage gaps**: Expanded unit tests from 19 to 40+ tests, achieving 51% coverage
- **Type safety**: Added mypy pre-commit hooks and resolved type-checking issues
- **Transform test stability**: Fixed logic and argument order issues in Polars transform tests

### Validation Reports

- Bronze profile report refreshed: `2026-01-13T17:16:06Z` (scope=all, profile_hash=`7a456...`)
- Silver quality checks: Partition-aware validation with lookback support
- Enriched quality validation: Business rule enforcement with detailed failure reports

## [0.3.0] - 2026-01-22 - Quality & Testing Sprint

### Features

- **Comprehensive Unit Tests**: Expanded coverage to 51% with new tests for transforms, runners, and validation modules
- **CI/CD Pipeline**: Full localized E2E pipeline check on sample data in GitHub Actions
- **Pre-commit Hooks**: Automated formatting (ruff), linting, and type-checking (mypy)
- **Code Quality Tools**: Integrated ruff for fast linting and formatting
- **Mock BigQuery Load Testing**: Validate parquet schema and format before actual BQ loads

### Improvements

- **Code Cleanup**: Final linting, formatting, and type-checking pass across entire codebase
- **Test Organization**: Reorganized tests into unit and integration test suites
- **Documentation Updates**: Validation framework documentation and runbooks

### Bug Fixes

- **Failing Unit Tests**: Resolved 8 failing tests related to import paths and type issues
- **Regex Patterns**: Fixed whitespace and regex issues in BQ testing tools
- **Transform Tests**: Corrected lazy evaluation and type-cast issues in Polars transforms
- **YAML Formatting**: Fixed whitespace issues in configuration files

## [0.2.0] - 2026-01-20 - Spec-Driven Architecture

### Features

- **Spec Models**: Pydantic models for pipeline specifications (`PipelineSpec`, `TableSpec`, `DimSpec`)
- **Layered Spec Configs**: Base spec in `config/specs/base.yml`, environment-specific overlays
- **Dynamic DAG Generation**: Airflow DAGs driven by spec configuration instead of hardcoded lists
- **Dimension Snapshot Runner**: `src.runners.dims_snapshot` with validation and freshness gates
- **Dims Freshness Gate**: BranchPythonOperator to skip dimension refresh if already current
- **Dims Snapshot Validation**: Lightweight quality gate for dimension tables (schema + PK integrity)

### Improvements

- **Airflow DAG Refactor**: `ecom_dim_refresh_pipeline` and `ecom_silver_to_gold_pipeline` now read from specs
- **Base Silver Models**: dbt models now read dimension snapshots from `SILVER_DIMS_PATH`
- **Configuration Strategy**: Added `ECOM_SPEC_PATH`, `SILVER_DIMS_PATH`, `SILVER_DIMS_LOCAL_PATH` env vars

### Bug Fixes

- **Dimension Path Resolution**: Proper GCS vs local path handling for dimension snapshots
- **Product Catalog Fallback**: Allow graceful fallback for missing dimension snapshots in dev

### Documentation

- **SPEC_OVERVIEW.md**: Comprehensive guide to spec-driven orchestration pattern
- **CONFIG_STRATEGY.md**: Updated with dimension snapshot environment variables
- **Dev Environment Alignment**: Documentation for local vs Docker/Airflow split

## [0.1.5] - 2026-01-18 - GCS Integration

### Features

- **GCS Sync Targets**: Makefile targets for syncing Silver and Enriched layers to GCS
- **GCP Auth Configuration**: Support for service account auth and ADC in Docker
- **GCS Path Resolution**: Smart path resolution with local fallbacks in enriched runners
- **Partition Lookback**: `lookback_days` parameter for Bronze/Silver validation modules
- **Cloud Auth Detection**: Observability modules detect auth method and route outputs to GCS
- **Comprehensive .env.example**: Complete GCP auth and pipeline configuration examples
- **Dev Pipeline Runner**: `scripts/run_dev_pipeline.sh` for local GCS testing

### Improvements

- **Config Enhancements**: Added `GCS_BUCKET`, `GCS_PREFIX`, partition metadata, and validation lookback settings
- **Validation Modules**: Enhanced with GCS support and partition lookback capabilities
- **Airflow DAGs**: Added GCP env vars and local path fallbacks for hybrid operation
- **Observability**: Support for GCS reports bucket in production environments

## [0.1.0] - 2026-01-16 - Major Refactor

### Features

- **Shared Scoring Logic**: `int_*_scored.sql` ephemeral models for consistent validation across base_silver
- **Enriched Silver Transforms**: 10 Polars-based transforms (customer_lifetime_value, daily_business_metrics, product_performance, shipping_economics, etc.)
- **Gold Mart Models**: 8 dbt-bigquery fact tables (cart_abandonment, customer_segments, etc.)
- **Modular Validation Framework**: Broke down monolithic scripts into packages (`src/validation/{bronze,silver,enriched}/`)
- **Common Validation Utilities**: Shared utilities in `src/validation/common.py`
- **Python Runner Module**: Replaced bash scripts with `src.runners.base_silver` Python module
- **Enhanced Makefile**: Detailed help menu with usage examples and variable requirements
- **Clean-data Target**: `make clean-data` for local data reset
- **CI Integration**: Added validation integration test step to CI pipeline

### Improvements

- **Docker Configuration**: Redirect dbt target/log paths to `/tmp` to avoid macOS VirtioFS file locking (Errno 35)
- **Named Volumes**: Use named volume for Airflow logs instead of bind mounts
- **Deterministic Dependencies**: Added `constraints.txt` for reproducible pip installs
- **dbt Models**: Extracted shared scoring logic into ephemeral CTEs, added `all_fields_null` fallback
- **Sources Configuration**: Moved `sources.yml` to `dbt_duckdb/models/` level

### Bug Fixes

- **Ingestion Metadata**: Consistently generate `ingestion_dt` across all base_silver models using `get_ingestion_dt` macro
- **Quarantine Analysis**: Filter out dbt-duckdb placeholder rows (null `row_num`) in validation
- **Enriched Schema Handling**: Strictly include `ingestion_dt` metadata in Silver schemas
- **Test Import Paths**: Updated test imports after validation refactor

### Removed

- **Duplicate dbt-BigQuery Python Models**: Deleted `int_attributed_purchases`, `int_customer_retention_signals`, `int_inventory_risk`, `int_regional_financials`, `int_sales_velocity` from dbt_bigquery (duplicated Polars runner logic)
  - **Rationale**: Polars runners are ~7x faster and ~$200/month cheaper than dbt-BigQuery Python models (see `docs/local_only/COST_ANALYSIS.md`)

### Documentation

- **Reorganized Documentation**: Created `docs/data/`, `docs/planning/`, `docs/resources/` subdirectories
- **Timestamped Validation Reports**: Added validation reports with timestamps to track quality over time

## [0.0.1] - 2026-01-09 - Initial Commit

### Project Scaffold

- **Base Silver**: dbt-duckdb models for data cleaning (8 staging tables + quarantine)
- **Enriched Silver**: Polars transforms for behavioral analytics (cart attribution, inventory risk, customer retention, sales velocity, regional financials)
- **Gold Marts**: dbt-bigquery aggregations for BI consumption (3 departmental marts)
- **Airflow Orchestration**: Table-level processing strategy with modular DAG design

### Planning Documentation

- `docs/planning/INTENT.md`: Project vision and "Rich Silver" philosophy
- `docs/planning/DECISIONS.md`: Architectural decision log
- `docs/planning/SILVER_FRAMEWORK.md`: Framework overview
- `docs/planning/SLA_AND_QUALITY.md`: Quality gates and SLA definitions
- `docs/planning/TESTING_RUNBOOK.md`: Testing strategy and runbook
- `docs/resources/DATA_CONTRACT.md`: Bronze → Silver type mapping contract

### Development Environment

- Conda environment setup
- Pre-commit hooks configuration
- Docker Compose for Airflow
- Bronze Parquet samples in `samples/bronze/` for local testing

### Project Philosophy

- **Hybrid Local-Cloud Compute**: Process 6 years of e-commerce data (~20GB) locally with Polars, minimize warehouse costs
- **Rich Silver Pattern**: Pre-compute behavioral attributes before cloud upload (40% estimated BigQuery cost reduction)
- **Modular Architecture**: Table-level processing enables parallelism and fits in <10GB memory footprint
- **Production-Ready Patterns**: Pydantic validation, idempotent tasks, error handling, lineage tracking

---

## Version History Summary

- **v1.0.0** (2026-01-23): Feature complete with spec-driven orchestration, three-layer validation, 51% test coverage
- **v0.3.0** (2026-01-22): Quality & testing sprint - comprehensive unit tests, CI/CD, code quality tools
- **v0.2.0** (2026-01-20): Spec-driven architecture with dynamic DAG generation and dimension snapshots
- **v0.1.5** (2026-01-18): GCS integration with cloud auth, partition lookback, and smart path resolution
- **v0.1.0** (2026-01-16): Major refactor - modular validation, enriched transforms, Docker fixes
- **v0.0.1** (2026-01-09): Initial commit - project scaffold, Rich Silver philosophy, planning docs

---

## Upcoming Features (Roadmap)

See [README.md - Future Enhancements](README.md#-future-enhancements) for the active roadmap:

- Incremental materialization for Silver transformations
- Data quality dashboard (load audit records into BigQuery)
- Great Expectations integration
- CI/CD for dbt (schema validation, automated deployment)
- dbt docs hosting on GitHub Pages
- Cost optimization (partition pruning, clustering)
- Workload Identity for production-grade auth

---

<p align="center">
  <a href="README.md">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="RESOURCE_HUB.md">📚 <b>Resource Hub</b></a>
</p>

<p align="center">
  <sub>Last updated: 2026-01-28</sub><br>
  <sub>✨ Transform the data. Tell the story. Build the future. ✨</sub>
</p>
