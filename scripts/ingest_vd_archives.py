import duckdb
import os
import requests
from datetime import datetime, timedelta, date
from pprint import pprint
import time
from pathlib import Path
import argparse
import sys

from common import retry

@retry(max_retries=3, backoff=5)
def download_and_insert(duckdb_con, curr_date_str, csv_path, uri):
    """
    Download CSV and insert into DuckDB for a single date
    Returns (success: bool, file_size: int)
    """

    print(f"Downloading {curr_date_str}.csv...")

    with requests.get(uri, stream=True) as r:
        r.raise_for_status()
        with open(csv_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    
    print(f"Downloaded {curr_date_str}.csv, ingesting...")

    query = f"""
        INSERT INTO staging.stg_version_downloads(version_id, downloads, date)
        SELECT version_id, downloads, '{curr_date_str}'::DATE as date
        FROM read_csv_auto('{csv_path}')
    """

    duckdb_con.execute(query)
    duckdb_con.execute("CHECKPOINT")

    print(f"Inserted data for {curr_date_str}! Removing from filesystem....\n\n")

    file_size = os.path.getsize(csv_path)
    os.remove(csv_path)

    return True, file_size


DUCKDB_PATH = 'data/crates.duckdb'
con = duckdb.connect(DUCKDB_PATH)

# TODO: Storage requirements for the 2019-today analytics

# System arguments for parsing how many days to backfill
parser = argparse.ArgumentParser()
parser.add_argument(
        "--backfill-days",
        type=int,
        help="Backfill for N days starting from min(date) in stg_version_downloads",
        default=None
)

parser.add_argument(
        "--backfill-to-date",
        type=str,
        help="Backfill up to this date starting from min(date) in stg_version_downloads (YYYY-MM-DD format)",
        default=None
)

args = parser.parse_args()

# Validate: can't use both
if args.backfill_days is not None and args.backfill_to_date is not None:
    print("ERROR: Cannot use both --backfill-days and --backfill-to-date")
    sys.exit(1)


start_date = con.execute("SELECT min(date) FROM staging.stg_version_downloads").fetchone()[0] - timedelta(days=1)
end_date = start_date - timedelta(days=30)

if args.backfill_to_date is not None:
    end_date = datetime.strptime(args.backfill_to_date, '%Y-%m-%d').date()

    if end_date >= start_date:
        print(f"ERROR: --backfill-to-date ({end_date}) must be before current min date ({start_date})")
        sys.exit(1)
elif args.backfill_days is not None:
    end_date = start_date - timedelta(days=args.backfill_days)



ARCHIVE_DIR = 'data/temp'

# Create if not exists dumps folder
data_dir = Path(ARCHIVE_DIR)
data_dir.mkdir(parents=True, exist_ok=True)

print(f"Backfilling from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")

curr_date = start_date
total_size = 0 # Calculates total size
first_duckdb_size = os.path.getsize(DUCKDB_PATH)
prev_duckdb_size = os.path.getsize(DUCKDB_PATH)

start = time.time()

# Exceptions: 2014-11-15 not existing
exception_dates = [
    '20141115'
]

while curr_date >= end_date:
    if curr_date.strftime('%Y%m%d') in exception_dates:
        curr_date -= timedelta(days=1)
        continue
        
    curr_date_str = curr_date.strftime('%Y-%m-%d')
    csv_path = os.path.join(ARCHIVE_DIR, f'{curr_date_str}.csv')
    uri = f'https://static.crates.io/archive/version-downloads/{curr_date_str}.csv'

    success, file_size = download_and_insert(con, curr_date_str, csv_path, uri)

    if not success:
        print(f"BACKFILL ABORTED: Failed to process {curr_date_str}!")
        con.close()
        sys.exit(1)
    
    # Calculate size and remove the file from the FS
    total_size += file_size
    duckdb_size = os.path.getsize(DUCKDB_PATH)

    curr_date -= timedelta(days=1)

    print(f"Total download size: {total_size / (1024 ** 2)}MB")
    print(f"DuckDB size: {duckdb_size / (1024 ** 2)}, previous: {prev_duckdb_size / (1024 ** 2)}")
    print(f"Time elapsed: {time.time() - start}\n")
    prev_duckdb_size = duckdb_size

con.close()

print(f"Starting DuckDB size: {first_duckdb_size / (1024 ** 2)} -> {prev_duckdb_size / (1024 ** 2)}, total downloaded: {total_size / (1024 ** 2)}")
print(f"Finished in {time.time() - start}")