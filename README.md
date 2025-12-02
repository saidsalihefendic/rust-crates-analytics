# Rust Crates Data Warehouse

This README will contain all the info regarding the project, its architecture, its tools and what we are accomplishing with each stage.

## Project setup

The easiest way to setup the project is simply running the command in the project root directory

```bash
./setup.sh
```

This will do all the work behind the scenes and create a workable DuckDB with crates.io dump db with all associated tables from the dump db in staging schema.

After the initial setup, when you decide to start playing with the project again, you can trigger the `update_duckdb.sh` script for updating to latest dump from crates.io. Bear in mind, this can delete some of the data about crates and versions (due to crates.io privacy policy)

```bash
./update_duckdb.sh
```

You can also load more history into version_downloads by triggering the backfill script in the project root directory

```bash
uv run scripts/ingest_vd_archives --backfill-days <INT>
```
where `--backfill-days` represents from minimum date in the `staging.stg_version_downloads`.

OR

```bash
uv run scripts/ingest_vd_archives --backfill-to-date <date>
```

where `--backfill-to-date` represents a date in YYYY-MM-DD. Will throw error if greater than min(date) from stg_version_downloads.


You can checkout the models in the dbt docs to receive more information on data discoverability and data quality checks. Simply run

```bash
cd transformations
uv run dbt docs generate && uv run dbt docs serve
```

## Architecture discussion

We will need to update README regarding the architecture of the project. The architecture will be a ELT strategy of loading the data into staging, then transforming and loading tables into marts tables for the dasbhoards. We also need to mention snapshotting and differential ingestion and slightly unusual approach of ingesting archives directly to staging, which breaks the ingestion pattern.

Project should be designed in modular fashion in such a way that we can upgrade the scales of our projects. The only immutable part is Python3 as our base for scripting and dbt for transformations and data governance, but all other can be replaced.

## Tools requirements

The tools that will realize this architecture, as PoC, will be 
- DuckDB for our DB
- Python3 scripts as our immutable base
- dbt for transformations and data quality checks, as well as minimum data governance
- Streamlit for data visualization

This needs to be discussed further and update the README of the project regarding the tools and architecture and why we went this way. The tools are decided because of the overhead cost of other tools that can be quite expensive in time and money efforts, while these are minimal tools that do the job as well for this project.

## Data Sources

- Crates db dump (90 day window in version_downloads, others are in all-time state): https://static.crates.io/db-dump.tar.gz
- Crates version_downloads historical archives: https://static.crates.io/archive/version-downloads/

**IMPORTANT**: We will need schematics for both architecture what, when and why, and other schematics for how with the tools and constraints

## Manual Ingestion and Transformation Flow

If you wish to do the project setup manually, follow this step by step guide to have your DuckDB loaded and ready for experimenting and usage

```bash
# Step 0. Setup uv project
uv sync

# Step 1. download the actual crates io dump db
uv run scripts/ingest_dump.py 

# If you already have dump, then you can just extract it again
uv run scripts/ingest_dump.py --skip-download

# Step 2. Create DuckDB filepath with three schemas: raw, staging and marts
uv run scripts/create_duckdb.py (to create DuckDB file with raw, staging and marts schemas, this is idempotent operation)

# Step 3. Load from extracted db dump crates.io to duckdb path, this will delete the extracted files
uv run scripts/load_duckdb.py

# Step 4. Run dbt transformations from raw -> staging
cd transformations
uv run dbt run --profiles-dir .
cd ..

# (OPTIONAL) Step 5. Backfill for number of days starting from min(date) from DuckDB file
uv run scripts/ingest_vd_archives.py --backfill-days [INT]
uv run scripts/ingest_vd_acrhives.py --backfill-to-date [YYYY-MM-DD]
```

You should be able to query data from raw and staging schema now, have a look around using a database client that supports DuckDB

The staging schema contains all the history of the crates.io
DuckDB handles big data quite nicely, it is a good show of how it behaves

## Run DuckDB MCP Server

Run

```bash
uv run mcp/mcp_duckdb_http.py
```

And add as HTTP transport MCP `http://127.0.0.1:8000/mcp`. As an example for Claude:

```bash
claude mcp add --transport http duckdb_crates http://127.0.0.1:8000/mcp
```

Verify it with

```bash
claude mcp list
```