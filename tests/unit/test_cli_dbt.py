"""Tests for CLI dbt helpers."""

import os
from unittest.mock import patch

from src.cli.app import _run_dbt_deps_locked


@patch("src.cli.app._run_cmd")
@patch("fcntl.flock")
@patch("src.cli.app.shutil.rmtree")
def test_run_dbt_deps_locked_logic(mock_rmtree, mock_flock, mock_run_cmd, tmp_path):
    """Test that _run_dbt_deps_locked cleans up specific folders and locks."""
    cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        # Setup dummy dbt_duckdb structure
        dbt_dir = tmp_path / "dbt_duckdb"
        dbt_dir.mkdir()
        packages_dir = dbt_dir / "dbt_packages"
        packages_dir.mkdir()

        # Dir to be deleted
        target_pkg = packages_dir / "dbt_utils 123"
        target_pkg.mkdir()

        # Dir to keep
        safe_pkg = packages_dir / "safe_pkg"
        safe_pkg.mkdir()

        # Log dir to be deleted
        log_dir = dbt_dir / "{{ env_var('DBT_LOG_PATH'"
        log_dir.mkdir()

        _run_dbt_deps_locked()

        # Verify rmtree called for target_pkg and log_dir
        # We check that it was called at least twice
        # (exact path matching is tricky with PosixPath vs str)
        assert mock_rmtree.call_count == 2

        # Verify run_cmd called
        mock_run_cmd.assert_called_once()
        args = mock_run_cmd.call_args[0][0]
        assert args == ["dbt", "deps", "--project-dir", ".", "--profiles-dir", "."]

    finally:
        os.chdir(cwd)
