from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class TableQualityMetrics:
    """Quality metrics for a single table."""

    table: str
    bronze_rows: int
    silver_rows: int
    quarantine_rows: int
    pass_rate: float
    sla_threshold: float
    status: str  # PASS, WARN, FAIL
    quarantine_breakdown: List[Dict[str, Any]]
    row_loss: int
    row_loss_pct: float


@dataclass
class SilverQualityReport:
    """Overall Silver layer quality report."""

    run_id: str
    timestamp: str
    table_metrics: List[TableQualityMetrics]
    fk_mismatch_summary: List[Dict[str, Any]]
    contract_issues: List[Dict[str, Any]]
    overall_status: str
    tables_passing: int
    tables_warning: int
    tables_failing: int
    total_quarantined: int
    total_processed: int
