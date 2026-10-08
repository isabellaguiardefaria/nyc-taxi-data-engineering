import duckdb

con = duckdb.connect()

result = con.execute("""
    SELECT
        COUNT(*) AS total_trips
    FROM 'data/raw/yellow_tripdata_2025-01.parquet'
""").fetchone()

print(f"Total de viagens: {result[0]:,}")