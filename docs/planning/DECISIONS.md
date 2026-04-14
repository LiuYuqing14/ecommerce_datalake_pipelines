# Decision Log

## Architecture & Design Decisions

| Date | Decision | Rationale | Impact | Owner | Status |
| --- | --- | --- | --- | --- | --- |
| 2026-01-09 | Medallion architecture with "Rich Silver" pattern | Traditional pipelines either dump raw data into warehouses or create simple cleaned layers without business logic. Rich Silver computes behavioral attributes (churn risk, attribution, velocity) locally before cloud upload | 40% estimated cost reduction in BigQuery scans; 7x faster transforms vs. warehouse-based Python models | Data Engineering | Approved |
| 2026-01-10 | Separate dbt projects for Base Silver (DuckDB) and Gold (BigQuery) | Clear boundary between local compute and warehouse models; Base Silver uses dbt-duckdb for fast local iteration, Gold uses dbt-bigquery for BI-optimized aggregations | Cleaner dependencies, faster dev cycles, warehouse cost isolation | Data Engineering, Platform Engineering | Approved |
| 2026-01-10 | Hybrid Polars modules with runner scripts (not dbt Python models) | Pure Polars logic in `src/transforms/`, I/O wrappers in `src/runners/`; dbt_bigquery only for Gold SQL models; all Enriched Silver computed locally and written to GCS before BQ load | Keeps all heavy compute local (no BQ costs), maintains testability, simpler than dbt Python models that don't fit local-first pattern | Data Engineering | Approved |
| 2026-01-16 | Delete duplicate dbt-BigQuery Python models | Initial design included both Polars runners and dbt-BigQuery Python models for enriched transforms. Benchmarking showed Polars was 7x faster and ~$200/month cheaper | Removed code duplication, simplified maintenance, validated cost-conscious architecture approach | Data Engineering | Approved |
| 2026-01-20 | Spec-driven orchestration with layered YAML configs | Pipeline configuration scattered across code, env vars, and hardcoded lists. Centralized table lists, partition keys, and validation gates in `config/specs/*.yml` | Single source of truth for pipeline metadata; enables dynamic DAG generation; easier to add new tables | Data Engineering, Platform Engineering | Approved |
| 2026-01-20 | Dimension snapshot pattern with freshness gate | Dimension tables (customers, products) change infrequently but are needed by all fact transforms. Snapshot daily and reuse rather than re-reading Bronze | 60% reduction in Bronze reads; faster DAG runs; dimension freshness validation prevents stale joins | Data Engineering | Approved |

## Data Partitioning & Processing

| Date | Decision | Rationale | Impact | Owner | Status |
| --- | --- | --- | --- | --- | --- |
| 2026-01-10 | Use daily partitions for Base Silver and Enriched Silver | Daily partitions align with downstream analytics granularity and simplify backfills, even when the historical ingest date is a single snapshot | Consistent partitioning strategy across layers; enables partition-level recovery | Data Engineering | Approved |
| 2026-01-10 | Table-level processing strategy (8 Base + 10 Enriched tasks) | Bronze backfill is one-time dump (not incremental by date); table-level chunking provides natural boundaries, enables parallelism, and fits in laptop memory (~6GB peak vs 20GB+ for full dataset) | Production-realistic architecture with <10GB memory footprint; each table processes independently | Data Engineering | Approved |
| 2026-01-18 | Add partition lookback for Bronze/Silver validation | Static Bronze partitions can't handle streaming/async ingestion scenarios. Add `lookback_days` parameter to pin specific Bronze partitions for each run | Prevents "ghost" FK misses in async scenarios; enables incremental processing patterns | Data Engineering | Approved |
| 2026-01-22 | Implement staging prefix pattern for GCS syncs | `gsutil rsync` is not atomic and can leave partial data during failures. Sync to `_staging/<run_id>/` prefix, write manifest, validate, then promote to canonical | Atomic publishes; versioned run folders; enables rollback; production-grade reliability | Data Engineering, Platform Engineering | Approved |

## Transform Logic & Business Rules

| Date | Decision | Rationale | Impact | Owner | Status |
| --- | --- | --- | --- | --- | --- |
| 2026-01-10 | Use `join_asof` for cart attribution | Temporal joins are efficient in Polars and avoid expensive window joins in SQL; links purchases to most recent cart within 48-hour window | 10x faster than SQL window functions; cleaner code; accurate attribution logic | Data Engineering, Business Intelligence | Approved |
| 2026-01-10 | Load enriched parquet to BigQuery before Gold marts | Reduces BQ compute costs by precomputing attributes locally; Gold marts become simple aggregations | Lower warehouse spend and faster mart builds; cleaner separation of transformation vs. serving | Data Engineering, Business Intelligence | Approved |
| 2026-01-16 | Extract shared scoring logic into ephemeral dbt models | All base_silver models duplicated scoring logic (null checks, invalid_reason). Refactor into `int_*_scored.sql` ephemeral CTEs | DRY principle; consistent scoring across all tables; easier to update validation rules | Data Engineering | Approved |
| 2026-01-22 | Three-layer validation framework (Bronze, Silver, Enriched) | Each layer has different quality requirements: Bronze checks manifests/schemas, Silver checks referential integrity, Enriched checks business rules | Fail fast at appropriate layer; detailed quality reports; prevents bad data propagation | Data Engineering, Data Quality | Approved |

## Infrastructure & Operations

| Date | Decision | Rationale | Impact | Owner | Status |
| --- | --- | --- | --- | --- | --- |
| 2026-01-10 | GCS ephemeral staging pattern (read → transform → write temp → upload → delete) | Student project requires cost minimization; storing 20GB locally defeats cloud-native simulation; ephemeral staging provides production-like GCS reads/writes with minimal local storage | Cloud-native architecture at ~$2-3/month cost; no persistent local data storage required | Data Engineering, Platform Engineering | Approved |
| 2026-01-16 | Redirect dbt target/log paths to `/tmp` in Docker | macOS VirtioFS file locking caused Errno 35 failures when dbt wrote compiled artifacts to bind-mounted volumes | Eliminated file locking issues; 100% DAG success rate; better Docker/macOS compatibility | Platform Engineering | Approved |
| 2026-01-17 | Replace `gsutil rsync` with `gcloud storage rsync` | `gsutil` is deprecated and lacks `--delete-unmatched-destination-objects` flag needed for clean syncs | Modern GCS tooling; atomic sync operations; future-proof infrastructure | Platform Engineering | Approved |
| 2026-01-18 | Add comprehensive GCS support with local fallbacks | Local dev and cloud deployment need different paths. Implement smart path resolution: GCS for prod, local filesystem for dev | Single codebase works in both environments; `PIPELINE_ENV` controls behavior; easier testing | Data Engineering, Platform Engineering | Approved |
| 2026-01-22 | Implement full E2E pipeline check in CI | Manual testing missed integration issues. Add automated Bronze→Silver→Enriched→Gold pipeline run on sample data | Catches breaking changes before merge; validates full pipeline; 51% test coverage achieved | Data Engineering, DevOps | Approved |

## Observability & Quality

| Date | Decision | Rationale | Impact | Owner | Status |
| --- | --- | --- | --- | --- | --- |
| 2026-01-16 | Break down monolithic validation scripts into modular packages | Initial validation scripts were 500+ line monoliths. Refactor into `src/validation/{bronze,silver,enriched}/` packages with shared utilities | Testable validation logic; reusable components; clearer responsibility boundaries | Data Engineering | Approved |
| 2026-01-18 | Add cloud auth detection and GCS reports bucket support | Observability metrics/logs written locally in dev, but should go to GCS in prod. Detect auth method and route outputs appropriately | Production-ready observability; metrics accessible in cloud; supports SLA dashboards | Data Engineering, SRE | Approved |
| 2026-01-21 | Write validation reports to GCS in production | Local validation reports aren't accessible to stakeholders. Publish to GCS bucket for visibility | Stakeholder transparency; historical quality tracking; audit trail for compliance | Data Engineering, Data Quality | Approved |
| 2026-01-22 | Add dimension snapshot validation without Bronze comparison | Dimensions are snapshots (not partitioned by Bronze ingest_dt). Validate schema and PK integrity without expensive historical scans | Lightweight quality gate; fast validation; prevents dimension drift | Data Engineering | Approved |

---

<p align="center">
  <a href="../../README.md">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="../../RESOURCE_HUB.md">📚 <b>Resource Hub</b></a>
</p>

<p align="center">
  <sub>Last updated: 2026-01-24</sub><br>
  <sub>✨ Transform the data. Tell the story. Build the future. ✨</sub>
</p>
