#!/bin/bash
# update_duckdb.sh - Update Crates.io Data Warehouse with latest data
# Run this whenever you want to sync with latest crates.io data
# Works whether run daily, weekly, or after any gap - dbt handles incremental logic
#
# Version: .1.0
# Last Updated: 2025-11-03

set -e # Exit on any error
set -o pipefail # Catch errors in pipes

VERSION="0.1.0"
LAST_UPDATED="2025-11-03"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Ensure we're in project root
if [ ! -f "pyproject.toml" ]; then
    log_error "Must run from project root directory"
    exit 1
fi

main() {
    echo ""
    echo "======================================"
    echo "  Crates.io Data Warehouse Update"
    echo "  Version: $VERSION ($LAST_UPDATED)"
    echo "======================================"
    echo ""
    echo "This will:"
    echo "  1. Download latest crates.io database dump"
    echo "  2. Recreate all raw tables from dump (crates, versions, etc.)"
    echo "  3. Checking the freshness of the updated raw schema"
    echo "  4. Running dbt transformations (incremental mode for version_downloads, others full refresh) and tests"
    echo ""
    echo "Estimated time: 5-10 minutes"
    echo ""

    echo ""
    log_info "[1/4] Downloading latest crates.io database dump..."
    uv run scripts/ingest_dump.py

    echo ""
    log_info "[2/4] Recreating raw tables and loading data into DuckDB..."
    uv run scripts/load_duckdb.py

    echo ""
    log_info "[3/4] Checking the freshness of the raw schema..."
    cd transformations
    uv run dbt source freshness --profiles-dir .
    cd ..

    echo ""
    log_info "[4/4] Running dbt transformations (incremental mode for version_downloads, others full refresh) and tests..."
    cd transformations
    uv run dbt build --profiles-dir .
    cd ..

    echo ""
    echo "======================================"
    echo -e "${GREEN}Update Complete!${NC}"
    echo "======================================"
    echo ""
    echo "Database updated with latest data."
    echo "Location: $(pwd)/crates.duckdb"
    echo ""
}

# Run main function
main