import duckdb
from pprint import pprint
import os
import shutil

def ingest_to_duckdb(duckdb_con, data_dir, csv_file):
    table_name = csv_file.split('.')[0]

    print(f"Creating table {table_name} from {os.path.join(data_dir, csv_file)}...")

    duckdb_con.execute(f'DROP TABLE IF EXISTS raw.{table_name}')
    duckdb_con.execute(f"""
        CREATE TABLE raw.{table_name} AS
        SELECT * FROM read_csv('{os.path.join(data_dir, csv_file)}', max_line_size=10000000)
    """)

    # Verify creation
    result = con.execute(f'SELECT COUNT(*) FROM raw.{table_name}').fetchone()
    pprint(result)

# Extracted data dir changes depending on the date fron ingest_dump script
EXTRACTED_DATA_DIR = "data/raw/extracted"
dirs = [d for d in os.listdir(os.path.join(EXTRACTED_DATA_DIR)) if os.path.isdir(os.path.join(EXTRACTED_DATA_DIR, d))]

if len(dirs) > 1 or len(dirs) == 0:
    raise Exception("Something went wrong with the data dumps from ingestion step")

DATA_DIR_PATH = os.path.join(EXTRACTED_DATA_DIR, dirs[0], 'data')

csv_files = [path for path in os.listdir(DATA_DIR_PATH) if '.csv' in path]

pprint(csv_files)

DUCKDB_PATH = 'data/crates.duckdb'

con = duckdb.connect(DUCKDB_PATH)

# Ingest each csv_file into raw_duckdb 
for csv_file in csv_files:
    ingest_to_duckdb(con, DATA_DIR_PATH, csv_file)

con.close()

# Clean up extracted utils
shutil.rmtree(EXTRACTED_DATA_DIR)
print(f"✓ Cleaned up {EXTRACTED_DATA_DIR}")