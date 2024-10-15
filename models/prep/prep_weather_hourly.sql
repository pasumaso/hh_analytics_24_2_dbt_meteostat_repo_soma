
WITH hourly_data AS (
    SELECT * 
    FROM {{ ref('staging_weather_hourly') }}
),
add_features AS (
    SELECT *
        , timestamp::DATE AS date -- Extract date from timestamp
        , timestamp::TIME AS time -- Extract only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') AS hour -- Extract time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMmonth') AS month_name -- Extract month name as text (e.g., 'January')
        , TO_CHAR(timestamp, 'FMDay') AS weekday -- Extract weekday name as text (e.g., 'Monday')
        , DATE_PART('day', timestamp) AS date_day -- Extract day of the month as an integer
        , DATE_PART('month', timestamp) AS date_month -- Extract month as an integer
        , DATE_PART('year', timestamp) AS date_year -- Extract year as an integer
        , EXTRACT(WEEK FROM timestamp) AS cw -- Extract calendar week (cw)
    FROM hourly_data
),
add_more_features AS (
    SELECT *
        , (CASE 
            WHEN time BETWEEN '00:00:00' AND '05:59:59' THEN 'night' -- Night: from midnight to 6 AM
            WHEN time BETWEEN '06:00:00' AND '11:59:59' THEN 'morning' -- Morning: from 6 AM to noon
            WHEN time BETWEEN '12:00:00' AND '17:59:59' THEN 'day' -- Day: from noon to 6 PM
            WHEN time BETWEEN '18:00:00' AND '23:59:59' THEN 'evening' -- Evening: from 6 PM to midnight
        END) AS day_part -- Categorize time of day into day parts
    FROM add_features
)

SELECT *
FROM add_more_features;
