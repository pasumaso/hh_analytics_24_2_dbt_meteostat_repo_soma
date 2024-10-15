WITH flights_one_month AS (
    SELECT * 
    FROM {{ ref('staging_flights_one_month') }}
),
flights_cleaned AS (
    SELECT flight_date::DATE
           , TO_CHAR(dep_time, 'fm0000')::TIME AS dep_time  -- Convert departure time to time format
           , TO_CHAR(sched_dep_time, 'fm0000')::TIME AS sched_dep_time -- Convert scheduled departure time
           , dep_delay
           , (dep_delay * '1 minute'::INTERVAL) AS dep_delay_interval -- Convert dep_delay to an interval
           , TO_CHAR(arr_time, 'fm0000')::TIME AS arr_time -- Convert arrival time to time format
           , TO_CHAR(sched_arr_time, 'fm0000')::TIME AS sched_arr_time -- Convert scheduled arrival time
           , arr_delay
           , (arr_delay * '1 minute'::INTERVAL) AS arr_delay_interval -- Convert arrival delay to an interval
           , airline
           , tail_number
           , flight_number
           , origin
           , dest
           , air_time
           , (air_time * '1 minute'::INTERVAL) AS air_time_interval -- Convert air time to an interval
           , actual_elapsed_time
           , (actual_elapsed_time * '1 minute'::INTERVAL) AS actual_elapsed_time_interval -- Convert actual elapsed time to an interval
           , (distance * 1.60934)::NUMERIC(6,2) AS distance_km -- Convert miles to kilometers (1 mile = 1.60934 km)
           , cancelled
           , diverted
    FROM flights_one_month
)
SELECT * 
FROM flights_cleaned
