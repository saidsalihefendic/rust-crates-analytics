import duckdb

DUCKDB_PATH = 'data/crates.duckdb'

con = duckdb.connect(DUCKDB_PATH)

con.execute('CREATE SCHEMA IF NOT EXISTS raw')
con.execute('CREATE SCHEMA IF NOT EXISTS staging')
con.execute('CREATE SCHEMA IF NOT EXISTS marts')

con.close()

print(f"✓ Database ready at {DUCKDB_PATH}")
print("✓ Schemas: raw, staging, marts")