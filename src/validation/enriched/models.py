from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class EnrichedTableMetrics:
    table: str
    partition_key: str
    ingest_dt: Optional[str]
    row_count: int
    min_rows: Optional[int]
    prior_row_count: Optional[int]
    row_delta_pct: Optional[float]
    schema_snapshot: Dict[str, str]
    null_rates: Dict[str, float]
    sanity_issues: List[str]
    semantic_issues: List[str]
    status: str  # PASS, WARN, FAIL
    notes: List[str]
