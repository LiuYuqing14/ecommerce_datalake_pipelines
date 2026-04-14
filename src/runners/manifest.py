"""Manifest generation utilities."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def generate_manifest(local_table_path: Path) -> None:
    """Generate _MANIFEST.json for each partition in a table (local only).

    Args:
        local_table_path: Path to the local table directory.
    """
    if not local_table_path.exists():
        logger.debug(f"Manifest skip: {local_table_path} does not exist.")
        return

    # Find all partitions (subdirectories)
    # Assumes Hive partitioning: table/key=value
    partitions = [p for p in local_table_path.iterdir() if p.is_dir() and "=" in p.name]

    # Also handle unpartitioned tables (root level parquet files)
    root_files = list(local_table_path.glob("*.parquet"))
    if root_files:
        partitions.append(local_table_path)

    if not partitions:
        logger.debug(f"No partitions or parquet files found in {local_table_path}")
        return

    for partition_dir in partitions:
        parquet_files = list(partition_dir.glob("*.parquet"))
        if not parquet_files:
            continue

        total_rows = 0
        files: list[dict[str, object]] = []

        for pfile in parquet_files:
            try:
                meta = pq.read_metadata(pfile)
                relative_path = str(pfile.relative_to(partition_dir))
                files.append(
                    {
                        "path": relative_path,
                        "rows": meta.num_rows,
                    }
                )
                total_rows += meta.num_rows
            except Exception as e:
                logger.warning(f"Failed to read metadata for {pfile}: {e}")

        manifest = {
            "total_rows": total_rows,
            "file_count": len(files),
            "files": files,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

        manifest_path = partition_dir / "_MANIFEST.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        logger.info(f"Generated manifest: {manifest_path}")
