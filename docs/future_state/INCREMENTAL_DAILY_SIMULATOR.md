# Incremental Daily Simulator

## Purpose
Extend the 6-year historical backfill with a 6-month incremental daily generation period to create less deterministic, more realistic data for analysis. This simulator respects the batch-oriented medallion architecture while introducing parameter variance, customer evolution, and controlled messiness.

## Design Goals
- **Batch-oriented**: Daily partitioned generation, NOT streaming or near-real-time
- **Non-deterministic**: Parameter drift over time creates realistic analytical variance
- **Customer evolution**: Read existing customer pool, generate new customers, update existing (SCD Type 2)
- **Campaign injection**: Specific events (Valentine's spike, new SKU launches, pricing experiments)
- **Controlled messiness**: Late arrivals, duplicates, schema micro-changes
- **Pipeline compatible**: Output to Bronze in same structure as backfill, processed by existing Silver/Gold layers

## Architecture Integration

### Existing Components (Reuse)
- **ecomlake CLI**: Extend with `generate-daily` command
- **Bronze structure**: `data/bronze/ecom/<table>/ingest_dt=YYYY-MM-DD/*.parquet`
- **Base Silver (dbt-duckdb)**: Processes new partitions with existing models
- **Enriched Silver (Polars runners)**: Applies transformations to new Base Silver data
- **Data Contract**: Same schema as backfill (v1), with optional new nullable columns

### New Components (Build)
- **Daily generator module**: `src/generators/incremental_daily.py`
- **Customer pool reader**: Reads last known state from Bronze/Base Silver
- **Parameter drift config**: YAML-based timeline of parameter changes
- **Campaign event config**: YAML-based event definitions (dates, targets, amplitudes)

## Daily Generation Flow

### 1. Initialization (One-time)
```bash
# Read final state from 6-year backfill
ecomlake init-incremental \
  --source-end-date 2025-12-31 \
  --extension-start-date 2026-01-01 \
  --extension-months 6
```

**Outputs:**
- `data/incremental/customer_pool_snapshot.parquet`: Latest customer state (deduped by customer_id, most recent record)
- `data/incremental/product_catalog_snapshot.parquet`: Latest product catalog
- `data/incremental/config/parameter_timeline.yml`: Template for drift config
- `data/incremental/config/campaign_events.yml`: Template for campaign config

### 2. Daily Run (Repeated)
```bash
# Generate single day's worth of data
ecomlake generate-daily \
  --date 2026-01-15 \
  --param-config data/incremental/config/parameter_timeline.yml \
  --campaign-config data/incremental/config/campaign_events.yml \
  --output-path data/bronze/ecom
```

**Process:**
1. Load customer pool snapshot
2. Load parameter config for target date
3. Check for campaign events on target date
4. Generate new orders (daily volume + campaign amplification)
5. Generate new customers (realistic signup rate)
6. Generate cart abandonment events
7. Generate returns (based on orders from 7-14 days prior)
8. Update product catalog (inventory changes, occasional new SKUs)
9. Write partitioned parquet to Bronze (same structure as backfill)
10. Update customer pool snapshot with new/updated customers

### 3. Orchestration (Airflow/Manual)
- **Backfill mode**: Generate all 180 days in sequence
- **Incremental mode**: Run daily after backfill, simulating "production" pattern
- Trigger existing Bronze → Silver → Gold pipeline per partition

## Parameter Drift Strategy

### Baseline Parameters (6-year backfill)
```yaml
# Stable baseline from historical generation
daily_order_volume: 500
daily_new_customers: 30
abandonment_rate: 0.65
return_rate: 0.08
loyalty_tier_distribution:
  bronze: 0.60
  silver: 0.25
  gold: 0.12
  platinum: 0.03
```

### Parameter Timeline (6-month extension)
```yaml
# data/incremental/config/parameter_timeline.yml
parameter_timeline:
  # Month 1: Slight increase in order volume, new customer surge
  - start_date: "2026-01-01"
    end_date: "2026-01-31"
    overrides:
      daily_order_volume: [480, 550]  # Random range per day
      daily_new_customers: [35, 50]   # Increased acquisition
      abandonment_rate: 0.67          # Slight uptick

  # Month 2: Valentine's baseline + event, return rate spike
  - start_date: "2026-02-01"
    end_date: "2026-02-28"
    overrides:
      daily_order_volume: [500, 600]
      return_rate: 0.10  # Post-holiday returns
      abandonment_rate: 0.63

  # Month 3: Price experiment month
  - start_date: "2026-03-01"
    end_date: "2026-03-31"
    overrides:
      daily_order_volume: [450, 520]
      discount_probability: 0.25  # Increased from 0.15
      abandonment_rate: 0.60  # Lower due to discounts

  # Month 4: New product category launch
  - start_date: "2026-04-01"
    end_date: "2026-04-30"
    overrides:
      daily_order_volume: [520, 580]
      new_sku_probability: 0.05  # Usually 0.01
      daily_new_customers: [40, 60]

  # Month 5: Summer slowdown
  - start_date: "2026-05-01"
    end_date: "2026-05-31"
    overrides:
      daily_order_volume: [400, 480]
      abandonment_rate: 0.70
      churn_rate: 0.02  # Usually 0.01

  # Month 6: Reactivation campaign
  - start_date: "2026-06-01"
    end_date: "2026-06-30"
    overrides:
      daily_order_volume: [550, 650]
      reactivation_probability: 0.15  # Usually 0.05
      loyalty_tier_upgrade_rate: 0.08  # Promotion incentives
```

## Campaign Event Injection

### Event Types
- **Volume spike**: Single-day or multi-day order amplification
- **SKU focus**: Promote specific product categories
- **Cohort targeting**: Email campaign to specific loyalty tiers
- **Pricing experiment**: Temporary discount on subset of products

### Example Campaign Config
```yaml
# data/incremental/config/campaign_events.yml
campaigns:
  # Valentine's Day Spike
  - name: "valentines_2026"
    start_date: "2026-02-14"
    end_date: "2026-02-14"
    effects:
      order_volume_multiplier: 2.5
      category_boost:
        "Home & Garden": 3.0  # Flowers, gifts
      abandonment_rate: 0.50  # Lower due to urgency

  # New Electronics Line Launch
  - name: "spring_electronics_launch"
    start_date: "2026-04-15"
    end_date: "2026-04-17"
    effects:
      new_skus:
        count: 12
        category: "Electronics"
        price_range: [199.99, 899.99]
      order_volume_multiplier: 1.8
      new_customer_multiplier: 2.0

  # Summer Sale
  - name: "summer_clearance_2026"
    start_date: "2026-06-15"
    end_date: "2026-06-21"
    effects:
      discount_probability: 0.60
      discount_amount_range: [0.20, 0.40]
      order_volume_multiplier: 2.2
      abandonment_rate: 0.55
```

## Customer Pool Evolution

### New Customer Generation
- Base rate: 30-60 new customers/day (varies by month)
- Campaign amplification: 2-3x during acquisition campaigns
- Attributes randomized per existing distribution
- Initial loyalty tier: Bronze (95%), Silver (5%)

### Existing Customer Updates (SCD Type 2)
- **Loyalty tier progression**: 5-8% monthly upgrade rate for active customers
- **Churn detection**: 1-2% monthly churn (customer_status = "churned")
- **Reactivation**: 5-15% reactivation probability for churned customers
- **Address changes**: 0.5% daily probability for active customers
- **Email changes**: 0.1% daily probability

### Implementation
```python
# Pseudocode for customer pool management
def generate_daily_customers(date, customer_pool, params):
    # New customers
    new_count = random.randint(params['daily_new_customers_range'])
    new_customers = [generate_new_customer(date) for _ in range(new_count)]

    # Existing customer updates
    active_pool = customer_pool[customer_pool['status'] == 'active']

    # Loyalty upgrades
    upgrade_candidates = active_pool.sample(frac=params['upgrade_rate'])
    upgraded_customers = [upgrade_tier(c) for c in upgrade_candidates]

    # Churn
    churn_candidates = active_pool.sample(frac=params['churn_rate'])
    churned_customers = [mark_churned(c, date) for c in churn_candidates]

    # Reactivations
    churned_pool = customer_pool[customer_pool['status'] == 'churned']
    reactivation_candidates = churned_pool.sample(frac=params['reactivation_probability'])
    reactivated_customers = [reactivate(c, date) for c in reactivation_candidates]

    # Combine and return
    return new_customers + upgraded_customers + churned_customers + reactivated_customers
```

## Controlled Messiness

### Late Arrivals (Backfill Simulation)
- 2% of orders arrive 1-3 days late
- Write to Bronze with `ingest_dt` offset from `order_date`
- Tests Silver idempotency and late-arrival handling

### Duplicates
- 0.5% duplicate order_id probability (different event_id)
- Tests deduplication logic in Base Silver dbt models

### Schema Micro-Changes
- Add 1-2 new nullable columns mid-way through extension (e.g., "utm_source", "referral_code")
- Tests additive schema evolution handling

### Data Quality Variance
- Occasional null values in optional fields (5% probability)
- Email format variations (with/without dots, plus addressing)
- Phone number format variations

## Output Structure

### Bronze Layer (Incremental Partitions)
```
data/bronze/ecom/
  orders/
    ingest_dt=2026-01-01/
      batch_20260101_120000.parquet
    ingest_dt=2026-01-02/
      batch_20260102_120000.parquet
    ...
  customers/
    ingest_dt=2026-01-01/
      batch_20260101_120000.parquet
      batch_20260101_120000_update.parquet  # SCD Type 2 updates
    ...
```

### Metadata Tracking
```json
// data/incremental/manifest.json
{
  "backfill_end_date": "2025-12-31",
  "extension_start_date": "2026-01-01",
  "extension_months": 6,
  "generated_partitions": [
    {"date": "2026-01-01", "status": "complete", "order_count": 523, "new_customers": 42},
    {"date": "2026-01-02", "status": "complete", "order_count": 498, "new_customers": 38},
    ...
  ],
  "campaign_events_applied": ["valentines_2026", "spring_electronics_launch"],
  "parameter_version": "v1.0"
}
```

## Testing Strategy

### Unit Tests
- Parameter drift config parsing
- Customer pool snapshot read/write
- Campaign event application logic
- SCD Type 2 update generation

### Integration Tests
- Generate 7-day sample period
- Verify Bronze schema compatibility
- Run Base Silver dbt models on sample
- Run Enriched Silver Polars transforms on sample
- Verify audit logs and lineage columns

### Validation Checks
- Order counts within expected range
- New customer counts within expected range
- No orphaned foreign keys (customer_id, product_id)
- Return dates follow order dates by 7-30 days
- Cart abandonment precedes orders by 0-48 hours
- Loyalty tier upgrades follow valid transitions

## Analytical Use Cases Enabled

### Time-Series Analysis
- Daily/weekly/monthly order trends with realistic variance
- Seasonal pattern detection with campaign spikes
- Customer acquisition cohort analysis

### Campaign Effectiveness
- Compare Valentine's Day performance vs baseline
- Measure new SKU launch impact on order volume
- Analyze discount experiment effect on abandonment rate

### Customer Lifecycle
- Churn prediction modeling with real churn events
- Loyalty tier progression analysis
- Reactivation campaign effectiveness

### Operational Analytics
- Late arrival impact on SLA metrics
- Duplicate detection effectiveness
- Schema evolution handling in pipeline

## Implementation Phases

### Phase 1: Core Daily Generator
- Build `ecomlake generate-daily` command
- Implement basic parameter drift (daily volume variance)
- Write to Bronze with same schema as backfill
- Test Base Silver processing

### Phase 2: Customer Pool Evolution
- Build customer snapshot reader
- Implement new customer generation
- Implement SCD Type 2 updates (tier upgrades, churn)
- Test customer lineage in Silver

### Phase 3: Campaign Events
- Build campaign config parser
- Implement volume amplification
- Implement category boosts
- Implement new SKU injection

### Phase 4: Controlled Messiness
- Add late arrival simulation
- Add duplicate generation
- Add schema evolution (new nullable columns)
- Test Silver robustness

### Phase 5: Orchestration
- Add Airflow DAG for daily generation
- Add backfill mode (generate 180 days sequentially)
- Add manifest tracking
- Add validation reports

## Configuration Examples

### Quick Start (Conservative)
```bash
# Generate 1 month with minimal drift
ecomlake generate-daily \
  --date-range 2026-01-01..2026-01-31 \
  --param-config config/conservative_params.yml \
  --no-campaigns \
  --no-late-arrivals
```

### Full Simulation (Realistic)
```bash
# Generate 6 months with all features
ecomlake generate-daily \
  --date-range 2026-01-01..2026-06-30 \
  --param-config data/incremental/config/parameter_timeline.yml \
  --campaign-config data/incremental/config/campaign_events.yml \
  --late-arrival-probability 0.02 \
  --duplicate-probability 0.005
```

### Single Day (Testing)
```bash
# Generate single day for pipeline testing
ecomlake generate-daily \
  --date 2026-02-14 \
  --param-config config/valentines_spike.yml \
  --campaign-config config/valentines_campaign.yml
```

## Success Metrics

### Technical
- 180 daily partitions generated successfully
- Zero schema contract violations
- All Base Silver dbt tests pass
- All Enriched Silver Polars transforms succeed
- Audit logs confirm lineage for all partitions

### Analytical
- Order volume variance > 15% (vs < 5% in deterministic backfill)
- Customer churn rate 1-2%/month (observable in retention analysis)
- Campaign events create visible spikes in time-series
- Late arrivals trigger idempotent re-processing
- SCD Type 2 updates create analyzable customer journey

## Notes
- Keep parameter drift realistic (no 10x spikes without campaign justification)
- Customer pool snapshot must be updated after each daily run (append-only for full history)
- Late arrivals should target random past partitions (1-3 days back, not 6 months)
- New SKUs must respect product_id uniqueness (increment from max existing)
- Campaign events should align with real-world ecommerce patterns (Valentine's, Back-to-School, Black Friday)
- All generated data must pass existing Silver validation rules (Pydantic schemas, dbt tests)

## Future Extensions
- Multi-region support (generate data for EMEA, APAC regions)
- Customer segment targeting (campaigns for Gold tier only)
- Product lifecycle (discontinue low-velocity SKUs, seasonal products)
- Returns pattern evolution (higher return rates for specific categories over time)
- Fraud event injection (test anomaly detection in Gold marts)

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
