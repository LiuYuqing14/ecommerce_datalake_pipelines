"""Base Silver Runner (dbt wrapper).

This module replaces scripts/run_base_silver.sh, providing a Pythonic interface
for executing dbt models with proper environment setup and path handling.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path

from src.runners.manifest import generate_manifest
from src.settings import load_settings
from src.specs import load_spec_safe

logger = logging.getLogger(__name__)

# Standard table list for directory creation
STANDARD_TABLES = [
    "customers",
    "product_catalog",
    "orders",
    "shopping_carts",
    "cart_items",
    "order_items",
    "returns",
    "return_items",
]


def extract_spec_path(argv: list[str]) -> tuple[str | None, list[str]]:
    """Extract --spec-path from argv and return cleaned args."""
    spec_path: str | None = None
    cleaned: list[str] = []
    skip_next = False
    for idx, arg in enumerate(argv):
        if skip_next:
            skip_next = False
            continue
        if arg.startswith("--spec-path"):
            if arg == "--spec-path":
                if idx + 1 < len(argv):
                    spec_path = argv[idx + 1]
                    skip_next = True
            else:
                _, value = arg.split("=", 1)
                spec_path = value
            continue
        cleaned.append(arg)
    return spec_path, cleaned


def has_select_arg(argv: list[str]) -> bool:
    """Return True if dbt selection is already present in argv."""
    for arg in argv:
        if arg in {"--select", "--models"}:
            return True
        if arg.startswith("--select=") or arg.startswith("--models="):
            return True
    return False


def check_virtiofs_deadlock(bronze_path: Path) -> None:
    """Detect MacOS Docker VirtioFS deadlock (Errno 35)."""
    # Only relevant for specific local paths in Docker
    if "/opt/airflow/samples" not in str(bronze_path):
        return

    if not bronze_path.exists():
        return

    try:
        # Try to read the first parquet file found
        found_files = list(bronze_path.glob("**/*.parquet"))
        if found_files:
            # Try reading 1 byte
            with open(found_files[0], "rb") as f:
                f.read(1)
    except OSError as e:
        if e.errno == 35:  # EDEADLK
            logger.error(
                "⚠️  VirtioFS read issue detected (Errno 35). "
                "Try: Docker Desktop → Settings → Change VirtioFS to gRPC FUSE"
            )
            sys.exit(1)
        raise


def ensure_local_directories(silver_path: Path, quarantine_path: Path) -> None:
    """Ensure output directories exist for local DuckDB writes."""
    if str(silver_path).startswith("gs://"):
        return

    logger.info("Ensuring local directory structure exists...")
    silver_path.mkdir(parents=True, exist_ok=True)
    quarantine_path.mkdir(parents=True, exist_ok=True)
    spec = load_spec_safe()
    table_names = [t.name for t in spec.silver_base.tables] if spec else STANDARD_TABLES
    for table in table_names:
        (silver_path / table).mkdir(parents=True, exist_ok=True)
        (quarantine_path / table).mkdir(parents=True, exist_ok=True)


def is_gcs_path(path: str) -> bool:
    """Check whether a path is a GCS URI."""
    return path.startswith("gs://")


def use_sa_auth() -> bool:
    """Check whether service-account auth is enabled."""
    return os.getenv("USE_SA_AUTH", "").lower() in {"1", "true", "yes", "on"}


def adc_credentials_path() -> Path:
    """Resolve the ADC credentials path for gcloud/gcsfs."""
    config_dir = os.getenv("CLOUDSDK_CONFIG", "").strip()
    if config_dir:
        return Path(config_dir) / "application_default_credentials.json"
    return Path.home() / ".config" / "gcloud" / "application_default_credentials.json"


def gcloud_rsync(source: str, destination: str, delete: bool) -> None:
    """Sync paths using gcloud storage rsync."""
    cmd = ["gcloud", "storage", "rsync", "-r"]
    if delete:
        cmd.append("--delete-unmatched-destination-objects")
    cmd.extend([source, destination])
    logger.info(f"Syncing {source} -> {destination}")
    subprocess.run(cmd, check=True, env=gcloud_env())


def gcloud_env() -> dict[str, str]:
    """Build gcloud environment overrides for auth."""
    env = os.environ.copy()
    creds = env.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if use_sa_auth() and creds:
        # Ensure gcloud uses the service account JSON without interactive login.
        env["CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"] = creds
    elif not use_sa_auth():
        adc_path = adc_credentials_path()
        if adc_path.exists():
            # Ensure gcloud uses ADC refresh token without interactive login.
            env["CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"] = str(adc_path)
    return env


def gcloud_copy_file(source: Path, destination: str) -> None:
    """Copy a local file to GCS using gcloud storage cp."""
    cmd = ["gcloud", "storage", "cp", str(source), destination]
    logger.info(f"Uploading {source} -> {destination}")
    subprocess.run(cmd, check=True, env=gcloud_env())


def resolve_run_id() -> str:
    """Resolve a stable run_id for publish staging."""
    for key in ("RUN_ID", "AIRFLOW_CTX_DAG_RUN_ID", "AIRFLOW_CTX_EXECUTION_DATE"):
        value = os.getenv(key, "").strip()
        if value:
            return value.replace(":", "").replace("+", "").replace(" ", "_")
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def run_dbt(
    project_dir: str = "dbt_duckdb",
    profiles_dir: str = "dbt_duckdb",
    target_path: str = "/tmp/dbt_target",
    log_path: str = "/tmp/dbt_logs",
    dbt_args: list[str] | None = None,
) -> None:
    """Execute dbt run with configured environment."""
    dbt_args = dbt_args or []

    # Ensure temp paths exist
    Path(target_path).mkdir(parents=True, exist_ok=True)
    Path(log_path).mkdir(parents=True, exist_ok=True)
    Path("/tmp/dbt_duckdb").mkdir(parents=True, exist_ok=True)

    # Prepare environment
    env = os.environ.copy()
    env["DBT_TARGET_PATH"] = target_path
    env["DBT_LOG_PATH"] = log_path
    env["DBT_PARTIAL_PARSE"] = "false"

    cmd = [
        "dbt",
        "run",
        "--project-dir",
        project_dir,
        "--profiles-dir",
        profiles_dir,
        "--no-partial-parse",
    ] + dbt_args

    logger.info(f"Executing: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            logger.error(
                f"dbt run failed with exit code {result.returncode}",
                extra={
                    "stdout": result.stdout[-500:] if result.stdout else "",
                    "stderr": result.stderr[-500:] if result.stderr else "",
                },
            )
            # Print full output for local debugging
            if result.stderr:
                print(f"\n--- dbt stderr ---\n{result.stderr}", file=sys.stderr)
            if result.stdout:
                print(f"\n--- dbt stdout ---\n{result.stdout}")
            sys.exit(result.returncode)

        # Success - log summary
        logger.info("dbt run completed successfully")
        if result.stdout:
            # Print output for visibility
            print(result.stdout)

    except Exception as e:
        logger.error(f"Unexpected error running dbt: {str(e)}")
        raise


def main() -> None:
    """Main entry point."""
    spec_path, dbt_extra_args = extract_spec_path(sys.argv[1:])
    if spec_path:
        os.environ["ECOM_SPEC_PATH"] = spec_path
    # Load settings to resolve defaults if env vars missing
    settings = load_settings()

    # Resolve paths (Env vars take precedence over config)
    airflow_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    spec = load_spec_safe(spec_path)
    bronze_path_raw = os.getenv("BRONZE_BASE_PATH") or (
        spec.bronze.base_path if spec else "samples/bronze"
    )
    silver_path_raw = os.getenv("SILVER_BASE_PATH") or (
        spec.silver_base.base_path if spec else "data/silver/base"
    )
    quarantine_path_raw = os.getenv("SILVER_QUARANTINE_PATH") or (
        spec.silver_base.quarantine_path if spec else f"{silver_path_raw}/quarantine"
    )

    bronze_path_effective = bronze_path_raw
    silver_path_effective = silver_path_raw
    quarantine_path_effective = quarantine_path_raw

    # Sync bronze from GCS to local
    if is_gcs_path(bronze_path_raw):
        local_bronze_root = os.getenv(
            "BRONZE_LOCAL_BASE_PATH", f"{airflow_home}/data/bronze"
        )
        Path(local_bronze_root).mkdir(parents=True, exist_ok=True)
        tables_env = os.getenv("BRONZE_SYNC_TABLES", "").strip()
        if tables_env:
            tables = [t.strip() for t in tables_env.split(",") if t.strip()]
            logger.info(
                "Syncing bronze tables from GCS to local",
                extra={"tables": tables},
            )
            for table in tables:
                gcloud_rsync(
                    f"{bronze_path_raw.rstrip('/')}/{table}",
                    str(Path(local_bronze_root) / table),
                    delete=True,
                )
        else:
            logger.info(
                "Syncing bronze from GCS to local: %s -> %s",
                bronze_path_raw,
                local_bronze_root,
            )
            gcloud_rsync(bronze_path_raw, local_bronze_root, delete=True)
        bronze_path_effective = local_bronze_root

    # Prepare local silver directory for GCS targets
    if is_gcs_path(silver_path_raw):
        local_silver_root = os.getenv(
            "SILVER_LOCAL_BASE_PATH", f"{airflow_home}/data/silver/base"
        )
        Path(local_silver_root).mkdir(parents=True, exist_ok=True)
        silver_path_effective = local_silver_root
        quarantine_path_effective = os.path.join(local_silver_root, "quarantine")

    os.environ["BRONZE_BASE_PATH"] = bronze_path_effective
    os.environ["SILVER_BASE_PATH"] = silver_path_effective
    os.environ["SILVER_QUARANTINE_PATH"] = quarantine_path_effective

    bronze_path = Path(bronze_path_effective)
    silver_path = Path(silver_path_effective)
    quarantine_path = Path(quarantine_path_effective)

    logger.info(f"Bronze Source: {bronze_path_effective}")
    logger.info(f"Silver Target: {silver_path_effective}")

    check_virtiofs_deadlock(bronze_path)
    ensure_local_directories(silver_path, quarantine_path)

    if not has_select_arg(dbt_extra_args) and spec:
        model_list = [
            table.dbt_model for table in spec.silver_base.tables if table.dbt_model
        ]
        if model_list:
            dbt_extra_args = ["--select", *model_list, *dbt_extra_args]

    run_dbt(dbt_args=dbt_extra_args)

    # Generate manifests for local output (always)
    # This ensures manifests exist for local validation and before any GCS sync
    tables_env = os.getenv("BRONZE_SYNC_TABLES", "").strip()
    if tables_env:
        table_names_local = [t.strip() for t in tables_env.split(",") if t.strip()]
    elif spec:
        table_names_local = [table.name for table in spec.silver_base.tables]
    else:
        table_names_local = [
            p.name
            for p in Path(silver_path_effective).iterdir()
            if p.is_dir() and p.name != "quarantine"
        ]

    for table in table_names_local:
        table_path = Path(silver_path_effective) / table
        if table_path.exists():
            generate_manifest(table_path)

    # Export local silver to GCS
    if is_gcs_path(silver_path_raw):
        export_base = os.getenv("SILVER_EXPORT_BASE_PATH", "").strip()
        pipeline_env = (
            os.getenv("PIPELINE_ENV") or settings.pipeline.environment or "local"
        ).lower()
        if not export_base and pipeline_env in {"dev", "prod"}:
            export_base = silver_path_raw

        if export_base and is_gcs_path(export_base):
            publish_mode = (
                os.getenv("SILVER_PUBLISH_MODE")
                or settings.pipeline.silver_publish_mode
                or "direct"
            ).lower()
            if publish_mode == "staging":
                run_id = resolve_run_id()
                staging_base = os.getenv("SILVER_STAGING_PATH", "").strip()
                if not staging_base:
                    staging_base = f"{export_base.rstrip('/')}/_staging/{run_id}"
                logger.info(
                    "Exporting local silver to staging: %s",
                    staging_base,
                )
                tables_env = os.getenv("BRONZE_SYNC_TABLES", "").strip()
                if tables_env:
                    table_names = [
                        t.strip() for t in tables_env.split(",") if t.strip()
                    ]
                elif spec:
                    table_names = [table.name for table in spec.silver_base.tables]
                else:
                    table_names = [
                        p.name
                        for p in Path(silver_path).iterdir()
                        if p.is_dir() and p.name != "quarantine"
                    ]

                for table in table_names:
                    source_table = Path(silver_path) / table
                    dest_table = f"{staging_base.rstrip('/')}/{table}"
                    if source_table.exists():
                        generate_manifest(source_table)
                        gcloud_rsync(str(source_table), dest_table, delete=True)

                    q_source = Path(quarantine_path) / table
                    q_dest = f"{staging_base.rstrip('/')}/quarantine/{table}"
                    if q_source.exists():
                        gcloud_rsync(str(q_source), q_dest, delete=True)

                manifest = {
                    "run_id": run_id,
                    "staging_path": staging_base,
                    "tables": table_names,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                }
                manifest_file = Path("/tmp") / "silver_manifest.json"
                manifest_file.write_text(json.dumps(manifest, indent=2))
                gcloud_copy_file(
                    manifest_file,
                    f"{staging_base}/_MANIFEST.json",
                )
                logger.info(
                    "Staged silver publish complete (promotion handled by pipeline).",
                    extra={"staging_path": staging_base},
                )
            else:
                tables_env = os.getenv("BRONZE_SYNC_TABLES", "").strip()
                if tables_env:
                    tables = [t.strip() for t in tables_env.split(",") if t.strip()]
                    logger.info(
                        "Exporting local silver to GCS (targeted): %s",
                        export_base,
                        extra={"tables": tables},
                    )
                    for table in tables:
                        # Sync Base Table
                        source_table = Path(silver_path) / table
                        dest_table = f"{export_base.rstrip('/')}/{table}"
                        if source_table.exists():
                            generate_manifest(source_table)
                            gcloud_rsync(str(source_table), dest_table, delete=True)

                        # Sync Quarantine Table (if exists)
                        q_source = Path(quarantine_path) / table
                        # Assuming quarantine path follows standard structure
                        # relative to export base. If export_base is .../silver/base,
                        # quarantine is .../silver/base/quarantine
                        q_dest = f"{export_base.rstrip('/')}/quarantine/{table}"
                        if q_source.exists():
                            gcloud_rsync(str(q_source), q_dest, delete=True)
                else:
                    logger.info("Exporting local silver to GCS (full): %s", export_base)
                    # Generate manifests for all tables in silver_path
                    for table_dir in Path(silver_path).iterdir():
                        if table_dir.is_dir() and table_dir.name != "quarantine":
                            dest_table = f"{export_base.rstrip('/')}/{table_dir.name}"
                            generate_manifest(table_dir)
                    gcloud_rsync(str(silver_path), export_base, delete=True)


if __name__ == "__main__":
    if not os.getenv("ECOM_CLI_SUPPRESS_DEPRECATION"):
        warnings.warn(
            (
                "Deprecated: use `ecomlake silver run` instead of "
                "python -m src.runners.base_silver"
            ),
            UserWarning,
            stacklevel=2,
        )
    logging.basicConfig(level=logging.INFO)
    main()
