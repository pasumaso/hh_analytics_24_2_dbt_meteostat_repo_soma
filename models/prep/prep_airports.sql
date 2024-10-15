WITH airports_reorder AS (
    SELECT  
    faa, 
    name, 
    country,
    region,
    lat,
    lon,
    alt,
    tz,
    dst,
    city    
    FROM {{ref('staging_airports')}}
)
SELECT * FROM airports_reorder