WITH daily_data AS (
    SELECT * 
    FROM {{ ref('staging_weather_daily') }}
),

-- Add date features
add_features AS (
    SELECT *
        , EXTRACT(DAY FROM date) AS date_day        -- Extract the day from the date
        , EXTRACT(MONTH FROM date) AS date_month    -- Extract the month from the date
        , EXTRACT(YEAR FROM date) AS date_year      -- Extract the year from the date
        , EXTRACT(WEEK FROM date) AS cw             -- Extract the calendar week from the date
        , TO_CHAR(date, 'Month') AS month_name      -- Extract the full month name
        , TO_CHAR(date, 'Day') AS weekday           -- Extract the full day of the week
    FROM daily_data
),

-- Add seasonal feature
add_more_features AS (
    SELECT *
        , (CASE 
            WHEN month_name IN ('December', 'January', 'February') THEN 'winter'    -- Define winter
            WHEN month_name IN ('March', 'April', 'May') THEN 'spring'              -- Define spring
            WHEN month_name IN ('June', 'July', 'August') THEN 'summer'             -- Define summer
            WHEN month_name IN ('September', 'October', 'November') THEN 'autumn'   -- Define autumn
        END) AS season
    FROM add_features
)

-- Final output
SELECT *
FROM add_more_features
ORDER BY date;
