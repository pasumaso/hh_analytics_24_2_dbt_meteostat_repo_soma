WITH flights_one_month AS (
    SELECT * 
    FROM 'staging_flights', 'flights'
    WHERE DATE_PART('month', flight_date) = 1 
)
SELECT * FROM flights_one_month