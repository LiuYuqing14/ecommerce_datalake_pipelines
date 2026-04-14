#!/usr/bin/env python3
"""Display pipeline version information from Git metadata baked into Docker image."""

import os
import subprocess
import sys
import warnings


def get_git_version():
    """Get version from Git (when running outside Docker)."""
    try:
        commit = (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL
            )
            .decode()
            .strip()
        )
        branch = (
            subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"], stderr=subprocess.DEVNULL
            )
            .decode()
            .strip()
        )
        try:
            tag = (
                subprocess.check_output(
                    ["git", "describe", "--tags", "--exact-match"],
                    stderr=subprocess.DEVNULL,
                )
                .decode()
                .strip()
            )
            version = tag
        except subprocess.CalledProcessError:
            version = f"{branch}-{commit}"
        return {"commit": commit, "branch": branch, "version": version, "source": "git"}
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def get_docker_version():
    """Get version from environment variables (set in Dockerfile)."""
    commit = os.getenv("GIT_COMMIT", "unknown")
    branch = os.getenv("GIT_BRANCH", "unknown")
    version = os.getenv("PIPELINE_VERSION", "unknown")

    if commit != "unknown":
        return {
            "commit": commit,
            "branch": branch,
            "version": version,
            "source": "docker",
        }
    return None


def main():
    """Display version information."""
    version_info = get_docker_version() or get_git_version()

    if not version_info:
        print("❌ Unable to determine version information")
        print("Not in a Git repository and Docker build args not set")
        sys.exit(1)

    print("=" * 60)
    print("E-commerce Data Pipeline - Version Information")
    print("=" * 60)
    print(f"Version:    {version_info['version']}")
    print(f"Git Commit: {version_info['commit']}")
    print(f"Git Branch: {version_info['branch']}")
    print(f"Source:     {version_info['source']}")
    print("=" * 60)

    if version_info["source"] == "git":
        print("\n💡 Running from local Git repository")
    else:
        print("\n🐳 Running from Docker image")


if __name__ == "__main__":
    if not os.getenv("ECOM_CLI_SUPPRESS_DEPRECATION"):
        warnings.warn(
            "Deprecated: use `ecomlake version` instead of scripts/version.py",
            UserWarning,
            stacklevel=2,
        )
    main()
