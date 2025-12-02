#!/usr/bin/env python3
"""update_duckdb.py - Update Crates.io DuckDB with latest data"""

import subprocess
import sys
from pathlib import Path

VERSION = "0.1.0"
LAST_UPDATED = "2025-12-02"

class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    NC = '\033[0m'
    
    @classmethod
    def disable_on_windows(cls):
        """Disable colors on Windows unless using Windows Terminal"""
        import platform
        if platform.system() == "Windows" and not sys.stdout.isatty():
            cls.RED = cls.GREEN = cls.YELLOW = cls.NC = ''

Colors.disable_on_windows()

def log_info(msg):
    print(f"{Colors.GREEN}[INFO]{Colors.NC} {msg}")

def log_error(msg):
    print(f"{Colors.RED}[ERROR]{Colors.NC} {msg}")

def log_warn(msg):
    print(f"{Colors.YELLOW}[WARN]{Colors.NC} {msg}")

def run_command(cmd, cwd=None):
    """Run command and exit on failure"""
    result = subprocess.run(cmd, cwd=cwd, shell=False)
    if result.returncode != 0:
        log_error(f"Command failed: {' '.join(cmd)}")
        sys.exit(1)

def main():
    # Ensure we're in project root
    if not Path("pyproject.toml").exists():
        log_error("Must run from project root directory")
        sys.exit(1)
    
    print("\n" + "="*40)
    print("  Crates.io Data Warehouse Update")
    print(f"  Version: {VERSION} ({LAST_UPDATED})")
    print("="*40 + "\n")
    
    print("This will:")
    print("  1. Download latest crates.io database dump")
    print("  2. Recreate all raw tables from dump (crates, versions, etc.)")
    print("  3. Check the freshness of the updated raw schema")
    print("  4. Run dbt transformations (incremental mode for version_downloads, others full refresh) and tests")
    print("\nEstimated time: 5-10 minutes\n")
    
    print()
    log_info("[1/4] Downloading latest crates.io database dump...")
    run_command(["uv", "run", "scripts/ingest_dump.py"])
    
    print()
    log_info("[2/4] Recreating raw tables and loading data into DuckDB...")
    run_command(["uv", "run", "scripts/load_duckdb.py"])
    
    print()
    log_info("[3/4] Checking the freshness of the raw schema...")
    run_command(["uv", "run", "dbt", "source", "freshness", "--profiles-dir", "."], cwd="transformations")
    
    print()
    log_info("[4/4] Running dbt transformations (incremental mode for version_downloads, others full refresh) and tests...")
    run_command(["uv", "run", "dbt", "build", "--profiles-dir", "."], cwd="transformations")
    
    print("\n" + "="*40)
    print(f"{Colors.GREEN}Update Complete!{Colors.NC}")
    print("="*40 + "\n")
    
    db_path = Path.cwd() / "crates.duckdb"
    print(f"Database updated with latest data.")
    print(f"Location: {db_path}\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nUpdate interrupted by user")
        sys.exit(1)