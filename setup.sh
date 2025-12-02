#!/bin/bash
# setup.sh - Autmated setup for Crates.io Data Warehouse
# This script sets up the entire project with 3 months of data (~10 minutes)

# Version: 0.1.0
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

check_prerequisites() {
    log_info "Checking prerequisites..."
    echo ""
    
    # Check uv
    if ! command -v uv &> /dev/null; then
        log_error "uv is required but not installed"
        echo "Install from: https://github.com/astral-sh/uv"
        exit 1
    fi
    UV_VERSION=$(uv --version 2>&1 | awk '{print $2}')
    echo "  - uv: $UV_VERSION"

    # Check disk space (need at least 10GB free)
    AVAILABLE=$(df . | tail -1 | awk '{print $4}')
    AVAILABLE_GB=$((AVAILABLE / 1024 / 1024))
    if [ "$AVAILABLE" -lt 10485760 ]; then  # 10GB in KB
        log_error "Need at least 10GB free disk space free disk space (have ${AVAILABLE_GB}GB)"
        exit 1
    fi
    echo "  - Disk space: ${AVAILABLE_GB}GB available"
    
    echo ""
    log_info "All prerequisites satisfied"
}

# Ensure we're in project root
if [ ! -f "pyproject.toml" ]; then
    log_error "Must run from project root directory"
    exit 1
fi

main() {
    echo ""
    echo "======================================"
    echo "  Crates.io Data Warehouse Setup"
    echo "  Version: $VERSION ($LAST_UPDATE)"
    echo "======================================"
    echo ""
    echo "This will:"
    echo "  0. Sync uv project (Python + dependencies)"
    echo "  1. Download crates.io database dump (will take ~2GB of storage)"
    echo "  2. Create DuckDB schemas"
    echo "  3. Load 3 months of data (will take ~6GB of storage)"
    echo "  4. Run dbt transformations and tests"
    echo ""
    echo "Estimated time: 5-15 minutes with crates.io dump db download"
    echo ""
    
    # Check if we should skip download
    SKIP_DOWNLOAD=""
    if [ -f "data/raw/db-dump.tar.gz" ]; then
        read -p "Found existing db-dump.tar.gz. Skip download? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            SKIP_DOWNLOAD="--skip-download"
            log_info "Will skip download and use existing file"
        fi
    fi
    
    check_prerequisites

    echo ""
    log_info "[0/4] Syncing uv project (Python + dependencies)..."
    uv sync

    echo ""
    log_info "[1/4] Downloading and extracting crates.io database dump..."
    if [ -n "$SKIP_DOWNLOAD" ]; then
        uv run scripts/ingest_dump.py $SKIP_DOWNLOAD
    else
        uv run scripts/ingest_dump.py
    fi

    echo ""
    log_info "[2/4] Creating DuckDB database with schemas..."
    uv run scripts/create_duckdb.py

    echo ""
    log_info "[3/4] Loading data into DuckDB..."
    uv run scripts/load_duckdb.py

    echo ""
    log_info "[4/4] Running dbt transformations and test..."
    cd transformations
    uv run dbt build --profiles-dir .
    cd ..

    echo ""
    echo "======================================"
    echo -e "${GREEN}Setup Complete!${NC}"
    echo "======================================"
    echo ""
    echo "Your database is ready with 3 months of data."
    echo "Location of DuckDB path: $(pwd)/crates.duckdb"
    echo ""
    echo "Next steps:"
    echo "  - Query data:    use DuckDB CLI or any database tool (DBeaver, DataGrip, etc.)"
    echo "  - Run dashboard: uv run streamlit run visualization/overview_dashboard.py"
    echo ""
}

# Run main function
main