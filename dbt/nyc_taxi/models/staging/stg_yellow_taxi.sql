SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount
FROM read_parquet(
    '/Users/isabellaguimaraes/projects/nyc-taxi-data-engineering/data/raw/yellow_tripdata_2025-01.parquet'
)