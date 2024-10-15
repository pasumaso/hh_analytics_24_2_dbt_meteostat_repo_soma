WITH departures AS (
                    SELECT origin AS faa
                            ,COUNT(pf.origin) AS nunique_from 
                            ,COUNT(pf.sched_dep_time) AS dep_planned -- how many flight were planned in total (departures)
                            ,SUM(pf.cancelled) AS dep_cancelled -- how many flights were canceled in total (departures)
                            ,SUM(pf.diverted) AS dep_diverted -- how many flights were diverted in total (departures)
                            ,COUNT(pf.arr_time) AS dep_n_flights-- how many flights actually occured in total (departures)
                            ,COUNT(DISTINCT pf.tail_number) AS dep_nunique_tails -- *(optional) how many unique airplanes travelled on average*
                            ,COUNT(DISTINCT pf.airline) AS dep_nunique_airlines -- *(optional) how many unique airlines were in service  on average*
                            ,pwd.min_temp_c as daily_min_temp_origin
                            ,pwd.max_temp_c as daily_max_temp_origin
                            ,pwd.precipitation_mm as daily_precipitation_origin
                            ,pwd.max_snow_mm as daily_snow_fall_origin
                            ,pwd.avg_wind_direction as daily_avg_wind_dir_origin
                            ,pwd.avg_wind_speed_kmh as daily_avg_wind_speed_origin
                            ,pwd.wind_peakgust_kmh as daily_wind_peakgust_origin
                        FROM {{ref('prep_flights')}} as pf
                        JOIN{{ref('prep_weather_daily')}} as pwd
                        on pf.origin = pwd.airport_code
                        GROUP BY origin, daily_min_temp_origin, daily_max_temp_origin, daily_precipitation_origin, daily_snow_fall_origin, daily_avg_wind_dir_origin, daily_avg_wind_speed_origin,daily_wind_peakgust_origin
),
-- unique number of arrival connections
arrivals AS (
                    SELECT dest AS faa
                            ,COUNT(pf.dest) AS nunique_to 
                            ,COUNT(pf.sched_dep_time) AS arr_planned -- how many flight were planned in total (arrivals)
                            ,SUM(pf.cancelled) AS arr_cancelled -- how many flights were canceled in total (arrivals)
                            ,SUM(pf.diverted) AS arr_diverted -- how many flights were diverted in total (arrivals)
                            ,COUNT(pf.arr_time)  AS arr_n_flights -- how many flights actually occured in total (arrivals)
                            ,COUNT(DISTINCT pf.tail_number) AS arr_nunique_tails -- *(optional) how many unique airplanes travelled on average*
                            ,COUNT(DISTINCT pf.airline) AS arr_nunique_airlines -- *(optional) how many unique airlines were in service  on average*
                            ,pwd.min_temp_c as daily_min_temp_dest
                            ,pwd.max_temp_c as daily_max_temp_dest
                            ,pwd.precipitation_mm as daily_precipitation_dest
                            ,pwd.max_snow_mm as daily_snow_fall_dest
                            ,pwd.avg_wind_direction as daily_avg_wind_dir_dest
                            ,pwd.avg_wind_speed_kmh as daily_avg_wind_speed_dest
                            ,pwd.wind_peakgust_kmh as daily_wind_peakgust_dest
                    FROM {{ref('prep_flights')}} as pf
                    JOIN{{ref('prep_weather_daily')}}as pwd
                    on pf.dest = pwd.airport_code
                    GROUP BY dest, daily_min_temp_dest, daily_max_temp_dest, daily_precipitation_dest, daily_snow_fall_dest, daily_avg_wind_dir_dest, daily_avg_wind_speed_dest, daily_wind_peakgust_dest
),
total_stats AS (
                    SELECT faa
                            ,nunique_to
                            ,nunique_from
                            ,dep_planned + arr_planned AS total_planed
                            ,dep_cancelled + arr_cancelled AS total_canceled
                            ,dep_diverted + arr_diverted AS total_diverted
                            ,dep_n_flights + arr_n_flights AS total_flights
                            ,daily_min_temp_origin
                            ,daily_max_temp_origin
                            ,daily_precipitation_origin
                            ,daily_snow_fall_origin
                            ,daily_avg_wind_dir_origin
                            ,daily_avg_wind_speed_origin
                            ,daily_wind_peakgust_origin
                            ,daily_min_temp_dest
                            ,daily_max_temp_dest
                            ,daily_precipitation_dest
                            ,daily_snow_fall_dest
                            ,daily_avg_wind_dir_dest
                            ,daily_avg_wind_speed_dest
                            ,daily_wind_peakgust_dest
                    FROM departures
                    JOIN arrivals
                    -- ON arrivals.faa = departures.faa
                    USING (faa)
)-- add city, country and name of the airport
SELECT city  
        ,country
        ,name
        ,total_stats.*
FROM {{ref('prep_airports')}}
RIGHT JOIN total_stats USING (faa)
ORDER BY city
