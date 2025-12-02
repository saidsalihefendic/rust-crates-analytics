#!/usr/bin/env python3
"""setup.py - Automated setup for Crates.io Data Warehouse"""

import subprocess
import sys
import shutil
import platform
from pathlib import Path

VERSION = "1.0"
LAST_UPDATED = "2025-12-02"

class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    NC = '\033[0m'
    
    @classmethod
    def disable_on_windows(cls):
        """Disable colors on Windows unless using Windows Terminal"""
        if platform.system() == "Windows" and not sys.stdout.isatty():
            cls.RED = cls.GREEN = cls.YELLOW = cls.NC = ''

Colors.disable_on_windows()

def log_info(msg):
    print(f"{Colors.GREEN}[INFO]{Colors.NC} {msg}")

def log_error(msg):
    print(f"{Colors.RED}[ERROR]{Colors.NC} {msg}")

def log_warn(msg):
    print(f"{Colors.YELLOW}[WARN]{Colors.NC} {msg}")

def check_prerequisites():
    log_info("Checking prerequisites...")
    
    # Check uv
    if not shutil.which("uv"):
        log_error("uv is required but not installed")
        print("Install from: https://github.com/astral-sh/uv")
        sys.exit(1)
    
    result = subprocess.run(["uv", "--version"], capture_output=True, text=True)
    uv_version = result.stdout.strip().split()[1]
    print(f"  - uv: {uv_version}")
    
    # Check disk space
    stat = shutil.disk_usage(".")
    available_gb = stat.free // (1024**3)
    if available_gb < 15:
        log_error(f"Need at least 15GB free disk space (have {available_gb}GB)")
        sys.exit(1)
    print(f"  - Disk space: {available_gb}GB available")
    
    log_info("All prerequisites satisfied")

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
    print("  Crates.io Analytics Setup")
    print(f"  Version: {VERSION} ({LAST_UPDATED})")
    print("="*40 + "\n")
    
    print("This will:")
    print("  0. Sync uv project (Python + dependencies)")
    print("  1. Download crates.io database dump (will take ~2GB of storage)")
    print("  2. Create DuckDB schemas")
    print("  3. Load 3 months of data (will take ~6GB of storage)")
    print("  4. Run dbt transformations and tests")
    print("\nEstimated time: 5-15 minutes with crates.io dump db download\n")
    
    # Check if we should skip download
    skip_download = False
    dump_path = Path("data/raw/db-dump.tar.gz")
    if dump_path.exists():
        response = input("Found existing db-dump.tar.gz. Skip download? (y/n): ").lower()
        if response == 'y':
            skip_download = True
            log_info("Will skip download and use existing file")
    
    check_prerequisites()
    
    print()
    log_info("[0/4] Syncing uv project (Python + dependencies)...")
    run_command(["uv", "sync"])
    
    print()
    log_info("[1/4] Downloading and extracting crates.io database dump...")
    cmd = ["uv", "run", "scripts/ingest_dump.py"]
    if skip_download:
        cmd.append("--skip-download")
    run_command(cmd)
    
    print()
    log_info("[2/4] Creating DuckDB database with schemas...")
    run_command(["uv", "run", "scripts/create_duckdb.py"])
    
    print()
    log_info("[3/4] Loading data into DuckDB...")
    run_command(["uv", "run", "scripts/load_duckdb.py"])
    
    print()
    log_info("[4/4] Running dbt transformations and tests...")
    run_command(["uv", "run", "dbt", "build", "--profiles-dir", "."], cwd="transformations")
    
    print("\n" + "="*40)
    print(f"{Colors.GREEN}Setup Complete!{Colors.NC}")
    print("="*40 + "\n")
    
    db_path = Path.cwd() / "crates.duckdb"
    print(f"Your database is ready with 3 months of data.")
    print(f"Location of DuckDB path: {db_path}\n")
    print("Next steps:")
    print("  - Query data:    use DuckDB CLI or any database tool (DBeaver, DataGrip, etc.)")
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nSetup interrupted by user")
        sys.exit(1)