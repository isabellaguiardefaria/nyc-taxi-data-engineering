SELECT
    payment_type,
    COUNT(*) AS total_trips,
    AVG(trip_distance) AS average_distance,
    AVG(fare_amount) AS average_fare,
    AVG(total_amount) AS average_total_amount
FROM {{ ref('stg_yellow_taxi') }}
GROUP BY payment_type
ORDER BY total_trips DESC