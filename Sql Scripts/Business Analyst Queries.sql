--- Business Analyst Queries

SELECT TOP 5
    g.city,
    g.country,
    l.location,
    AVG(fa.value) AS avg_pm25
FROM
    AirQualityData.Production.fact_aq AS fa
JOIN
    AirQualityData.Production.dim_locations AS l ON fa.location_id = l.id
JOIN
    AirQualityData.Production.dim_geo AS g ON l.geo_id = g.id
JOIN
    AirQualityData.Production.dim_parameter AS p ON fa.parameter_id = p.id
WHERE
    p.parameter = 'pm25'
	AND CONVERT(DATE, fa.utc) = '2018-04-04'
GROUP BY
    g.city, g.country, l.location
ORDER BY
    avg_pm25 DESC;


 with averagevalues as (  
		SELECT
        g.city,
        g.country,
        l.location,
        p.parameter,
        AVG(fa.value) AS avg_value
    FROM
        AirQualityData.Production.fact_aq AS fa
    JOIN
        AirQualityData.Production.dim_locations AS l ON fa.location_id = l.id
    JOIN
        AirQualityData.Production.dim_geo AS g ON l.geo_id = g.id
    JOIN
        AirQualityData.Production.dim_parameter AS p ON fa.parameter_id = p.id
    WHERE
        p.parameter IN ('co', 'so2')  -- Carbon monoxide and sulphur dioxide
        AND CONVERT(VARCHAR(7), fa.utc, 120) = '2018-04'
    GROUP BY
        g.city, g.country, l.location, p.parameter
)
Select * from 
averagevalues
where avg_value >= (select PERCENTILE_CONT (0.9) within group (order by avg_value))



WITH AQI_Calculation AS (
    SELECT
        g.country,
        AVG(CASE WHEN p.parameter = 'pm25' THEN fa.value ELSE NULL END) AS avg_pm25,
        AVG(CASE WHEN p.parameter = 'so2' THEN fa.value ELSE NULL END) AS avg_so2,
        AVG(CASE WHEN p.parameter = 'co' THEN fa.value ELSE NULL END) AS avg_co
    FROM
        AirQualityData.Production.fact_aq AS fa
    LEFT JOIN
        AirQualityData.Production.dim_locations AS l ON fa.location_id = l.id
    LEFT JOIN
        AirQualityData.Production.dim_geo AS g ON l.geo_id = g.id
    LEFT JOIN
        AirQualityData.Production.dim_parameter AS p ON fa.parameter_id = p.id
    WHERE
        fa.utc BETWEEN '2018-04-04T06:00:00.000Z' AND '2018-04-04T06:59:59.999Z'  
    GROUP BY
        g.country
)
SELECT
    country,
    CASE
        WHEN avg_pm25 <= 15
        OR     avg_so2 <= 0.5
        OR     avg_co <= 4.4 THEN 'Low'
        WHEN avg_pm25 <= 30
        OR     avg_so2 BETWEEN 0.6 AND    0.9
        OR     avg_co BETWEEN 4.5 AND    9.4 THEN 'Moderate'
        ELSE 'High'
    END AS air_quality_index  
FROM
    AQI_Calculation;


WITH daily_avg_pollution AS
(
          SELECT    loc.city,
                    Ifnull(Avg(
                    CASE
                              WHEN param.parameter = 'pm25' THEN Ifnull(value,0)
                    END),0) AS avg_pm25
          FROM      AirQualityData.Production.fact_aq fct
          LEFT JOIN AirQualityData.Production.dim_locations loc
          ON        fct.location_id = loc.location_id
          LEFT JOIN AirQualityData.Production.dim_parameter param
          ON        fct.parameter_id = param.parameter_id
          WHERE     Date_trunc('day', utc_timestamp) = $day  -- Replace  with the desired date
          AND       Date_part('hour', utc_timestamp) = $hour -- Replace hour with the desired hour
          GROUP BY  city ), pollution_stats_of_cities AS
(
         SELECT   city,
                  Sum(
                  CASE
                           WHEN parameter = 'co' THEN mean
                           ELSE 0
                  END) AS co_mean ,
                  Sum(
                  CASE
                           WHEN parameter = 'co' THEN median
                           ELSE 0
                  END) AS co_median ,
                  Sum(
                  CASE
                           WHEN parameter = 'co' THEN mode
                           ELSE 0
                  END) AS co_mode ,
                  Sum(
                  CASE
                           WHEN parameter = 'so2' THEN mean
                           ELSE 0
                  END) AS so2_mean ,
                  Sum(
                  CASE
                           WHEN parameter = 'so2' THEN median
                           ELSE 0
                  END) AS so2_median ,
                  Sum(
                  CASE
                           WHEN parameter = 'so2' THEN mode
                           ELSE 0
                  END) AS so2_mode
         FROM     (
                            SELECT    loc.city,
                                      param.parameter,
                                      Avg(fct.value)                                         AS mean,
                                      Percentile_cont(0.5) within GROUP (ORDER BY fct.value) AS median,
                                      mode(fct.value)                                        AS mode
                            FROM      AirQualityData.Production.fact_aq fct
                            LEFT JOIN AirQualityData.Production.dim_locations loc
                            ON        fct.location_id = loc.location_id
                            LEFT JOIN AirQualityData.Production.dim_parameter param
                            ON        fct.parameter_id = param.parameter_id
                            WHERE     date_trunc('day', fct.utc_timestamp) = $day
                            GROUP BY  loc.city,
                                      param.parameter )
         GROUP BY city )
SELECT    rc.city,
          rc.avg_pm25,
          psc.co_mean,
          psc.co_median,
          psc.co_mode,
          psc.so2_mean,
          psc.so2_median,
          psc.so2_mode
FROM      (
                   SELECT   city,
                            avg_pm25,
                            row_number() OVER (ORDER BY avg_pm25 DESC) AS rank
                   FROM     daily_avg_pollution ) AS rc
LEFT JOIN pollution_stats_of_cities psc
ON        rc.city = psc.city
WHERE     rank <= 10;
