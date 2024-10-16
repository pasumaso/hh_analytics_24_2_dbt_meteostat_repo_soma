WITH departures AS (
                    SELECT origin AS faa
                            ,flight_date
                            ,COUNT(origin) AS nunique_from 
                            ,COUNT(sched_dep_time) AS dep_planned -- how many flight were planned in total (departures)
                            ,SUM(cancelled) AS dep_cancelled -- how many flights were canceled in total (departures)
                            ,SUM(diverted) AS dep_diverted -- how many flights were diverted in total (departures)
                            ,COUNT(arr_time) AS dep_n_flights-- how many flights actually occured in total (departures)
                            ,COUNT(DISTINCT tail_number) AS dep_nunique_tails -- *(optional) how many unique airplanes travelled on average*
                            ,COUNT(DISTINCT airline) AS dep_nunique_airlines -- *(optional) how many unique airlines were in service  on average* 
                    FROM {{ref('prep_flights')}}
                    GROUP BY origin, flight_date
                    ORDER BY origin, flight_date
),
-- unique number of arrival connections
arrivals AS (
                    SELECT dest AS faa
                            ,flight_date
                            ,COUNT(dest) AS nunique_to 
                            ,COUNT(sched_dep_time) AS arr_planned -- how many flight were planned in total (arrivals)
                            ,SUM(cancelled) AS arr_cancelled -- how many flights were canceled in total (arrivals)
                            ,SUM(diverted) AS arr_diverted -- how many flights were diverted in total (arrivals)
                            ,COUNT(arr_time)  AS arr_n_flights -- how many flights actually occured in total (arrivals)
                            ,COUNT(DISTINCT tail_number) AS arr_nunique_tails -- *(optional) how many unique airplanes travelled on average*
                            ,COUNT(DISTINCT airline) AS arr_nunique_airlines -- *(optional) how many unique airlines were in service  on average* 
                    FROM {{ref('prep_flights')}}
                    GROUP BY dest, flight_date
                    ORDER BY dest, flight_date
),
total_stats AS (
                    SELECT faa
                            ,flight_date
                            ,nunique_to
                            ,nunique_from
                            ,dep_planned + arr_planned AS total_planed
                            ,dep_cancelled + arr_cancelled AS total_canceled
                            ,dep_diverted + arr_diverted AS total_diverted
                            ,dep_n_flights + arr_n_flights AS total_flights
                    FROM departures
                    JOIN arrivals
                    -- ON arrivals.faa = departures.faa
                    USING (faa)
),
daily_selected_stats AS (
                         SELECT 
                         airport_code as faa
                        ,min_temp_c as daily_min_temp
                        ,max_temp_c as daily_max_temp
                        ,precipitation_mm as daily_precipitation
                        ,max_snow_mm as daily_snow_fall
                        ,avg_wind_direction as daily_avg_wind_dir
                        ,avg_wind_speed_kmh as daily_avg_wind_speed
                        ,wind_peakgust_kmh as daily_wind_peakgust
                    FROM {{ref('prep_weather_daily')}}
                    JOIN total_stats
                USING (faa)
)-- add city, country and name of the airport
SELECT city  
        ,country
        ,name
        ,total_stats.*
        ,daily_selected_stats.*
FROM {{ref('prep_airports')}}
RIGHT JOIN total_stats
USING (faa)
ORDER BY city