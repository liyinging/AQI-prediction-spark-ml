SELECT 
    *,
    -- LEAD(max_aqi) OVER (PARTITION BY county_code, city_name, cbsa_name ORDER BY year, month, day ASC) AS max_aqi_tomorrow,
    -- LAG(max_aqi, 1) OVER (PARTITION BY county_code, city_name, cbsa_name ORDER BY year, month, day ASC) AS max_aqi_yesterday,
    -- LAG(max_aqi, 2) OVER (PARTITION BY county_code, city_name, cbsa_name ORDER BY year, month, day ASC) AS max_aqi_before_yesterday
    LEAD(max_aqi) OVER (PARTITION BY county_code, city_name ORDER BY year, month, day ASC) AS max_aqi_tomorrow,
    LAG(max_aqi, 1) OVER (PARTITION BY county_code, city_name ORDER BY year, month, day ASC) AS max_aqi_yesterday,
    LAG(max_aqi, 2) OVER (PARTITION BY county_code, city_name ORDER BY year, month, day ASC) AS max_aqi_before_yesterday
FROM(

    SELECT
        COALESCE(county_code_co, county_code_no2, county_code_o3, county_code_pm10, county_code_pm25_frm, county_code_pm25_nonfrm,
                county_code_pm25_speciation, county_code_pressure, county_code_so2, county_code_temp, county_code_wind) 
                AS county_code,

        COALESCE(city_name_co, city_name_no2, city_name_o3, city_name_pm10, city_name_pm25_frm, city_name_pm25_nonfrm, 
                city_name_pm25_speciation, city_name_pressure, city_name_so2, city_name_temp, city_name_wind) 
                AS city_name,
        
        -- COALESCE(cbsa_name_co, cbsa_name_no2, cbsa_name_o3, cbsa_name_pm10, cbsa_name_pm25_frm, cbsa_name_pm25_nonfrm,
        --         cbsa_name_pm25_speciation, cbsa_name_pressure, cbsa_name_so2, cbsa_name_temp, cbsa_name_wind) 
        --         AS cbsa_name,
        
        COALESCE(year_co, year_no2, year_o3, year_pm10, year_pm25_frm, year_pm25_nonfrm, year_pm25_speciation, 
                year_pressure, year_so2, year_temp, year_wind) AS year,
        COALESCE(month_co, month_no2, month_o3, month_pm10, month_pm25_frm, month_pm25_nonfrm, month_pm25_speciation,
                month_pressure, month_so2, month_temp, month_wind) AS month,
        COALESCE(day_co, day_no2, day_o3, day_pm10, day_pm25_frm, day_pm25_nonfrm, day_pm25_speciation,
                day_pressure, day_so2, day_temp, day_wind) AS day,

        AVG(COALESCE(longitude_co, longitude_no2, longitude_o3, longitude_pm10, longitude_pm25_frm, longitude_pm25_nonfrm,
                longitude_pm25_speciation, longitude_pressure, longitude_so2, longitude_temp, longitude_wind))
                AS longitude,

        AVG(COALESCE(latitude_co, latitude_no2, latitude_o3, latitude_pm10, latitude_pm25_frm, latitude_pm25_nonfrm, 
                latitude_pm25_speciation, latitude_pressure, latitude_so2, latitude_temp, latitude_wind))
                AS latitude,

        COUNT(COALESCE(site_num_co, site_num_no2, site_num_o3, site_num_pm10, site_num_pm25_frm, site_num_pm25_nonfrm,
                site_num_pm25_speciation, site_num_pressure, site_num_so2, site_num_temp, site_num_wind))
                AS site_num,

        MAX(COALESCE(dayofweek_co, dayofweek_no2, dayofweek_o3, dayofweek_pm10, dayofweek_pm25_frm, day_pm25_nonfrm,
                dayofweek_pm25_speciation, dayofweek_pressure, dayofweek_so2, dayofweek_temp, dayofweek_wind)) 
                AS dayofweek,

        AVG(arithmetic_mean_co) AS arithmetic_mean_co, 
        AVG(arithmetic_mean_no2) AS arithmetic_mean_no2, 
        AVG(arithmetic_mean_o3) AS arithmetic_mean_o3, 
        AVG(arithmetic_mean_pm10) AS arithmetic_mean_pm10, 
        AVG(arithmetic_mean_pm25_frm) AS arithmetic_mean_pm25_frm,
        AVG(arithmetic_mean_pm25_nonfrm) AS arithmetic_mean_pm25_nonfrm, 
        AVG(arithmetic_mean_pm25_speciation) AS arithmetic_mean_pm25_speciation, 
        AVG(arithmetic_mean_pressure) AS arithmetic_mean_pressure, 
        AVG(arithmetic_mean_so2) AS arithmetic_mean_so2, 
        AVG(arithmetic_mean_temp) AS arithmetic_mean_temp, 
        AVG(arithmetic_mean_wind) AS arithmetic_mean_wind,

        MAX(aqi_co) AS aqi_co, 
        MAX(aqi_no2) AS aqi_no2, 
        MAX(aqi_o3) AS aqi_o3, 
        MAX(aqi_pm10) AS aqi_pm10, 
        MAX(aqi_pm25_frm) AS aqi_pm25_frm, 
        MAX(aqi_pm25_nonfrm) AS aqi_pm25_nonfrm, 
        MAX(aqi_pm25_speciation) AS aqi_pm25_speciation, 
        MAX(aqi_so2) AS aqi_so2,

        -- first_max_hour_co, 
        -- first_max_hour_no2, 
        -- first_max_hour_o3, 
        -- first_max_hour_pm10, 
        -- first_max_hour_pm25_frm, 
        -- first_max_hour_p25_nonfrm, 
        -- first_max_hour_p25_speciation, 
        -- first_max_hour_pressure, 
        -- first_max_hour_so2,
        -- first_max_hour_temp, 
        -- first_max_hour_wind,

        MAX(first_max_value_co) AS first_max_value_co, 
        MAX(first_max_value_no2) AS first_max_value_no2, 
        MAX(first_max_value_o3) AS first_max_value_o3, 
        MAX(first_max_value_pm10) AS first_max_value_pm10, 
        MAX(first_max_value_pm25_frm) AS first_max_value_pm25_frm,
        MAX(first_max_value_pm25_nonfrm) AS first_max_value_pm25_nonfrm, 
        MAX(first_max_value_pm25_speciation) AS first_max_value_pm25_speciation, 
        MAX(first_max_value_pressure) AS first_max_value_pressure,
        MAX(first_max_value_so2) AS first_max_value_so2, 
        MAX(first_max_value_temp) AS first_max_value_temp, 
        MAX(first_max_value_wind) AS first_max_value_wind,

        SUM(observation_count_co) AS observation_count_co, 
        SUM(observation_count_no2) AS observation_count_no2, 
        SUM(observation_count_o3) AS observation_count_o3, 
        SUM(observation_count_pm10) AS observation_count_pm10, 
        SUM(observation_count_pm25_frm) AS observation_count_pm25_frm,
        SUM(observation_count_pm25_nonfrm) AS observation_count_pm25_nonfrm, 
        SUM(observation_count_pm25_speciation) AS observation_count_pm25_speciation, 
        SUM(observation_count_pressure) AS observation_count_pressure, 
        SUM(observation_count_so2) AS observation_count_so2,
        SUM(observation_count_temp) AS observation_count_temp, 
        SUM(observation_count_wind) AS observation_count_wind,

        MAX(GREATEST(
            COALESCE(aqi_co, aqi_no2, aqi_o3, aqi_pm10, aqi_pm25_frm, aqi_pm25_nonfrm, aqi_pm25_speciation, aqi_so2),
            COALESCE(aqi_no2, aqi_o3, aqi_pm10, aqi_pm25_frm, aqi_pm25_nonfrm, aqi_pm25_speciation, aqi_so2, aqi_co),
            COALESCE(aqi_o3, aqi_pm10, aqi_pm25_frm, aqi_pm25_nonfrm, aqi_pm25_speciation, aqi_so2, aqi_co, aqi_no2),
            COALESCE(aqi_pm10, aqi_pm25_frm, aqi_pm25_nonfrm, aqi_pm25_speciation, aqi_so2, aqi_co, aqi_no2, aqi_o3),
            COALESCE(aqi_pm25_frm, aqi_pm25_nonfrm, aqi_pm25_speciation, aqi_so2, aqi_co, aqi_no2, aqi_o3, aqi_pm10),
            COALESCE(aqi_pm25_nonfrm, aqi_pm25_speciation, aqi_so2, aqi_co, aqi_no2, aqi_o3, aqi_pm10, aqi_pm25_frm),
            COALESCE(aqi_pm25_speciation, aqi_so2, aqi_co, aqi_no2, aqi_o3, aqi_pm10, aqi_pm25_frm, aqi_pm25_nonfrm),
            COALESCE(aqi_so2, aqi_co, aqi_no2, aqi_o3, aqi_pm10, aqi_pm25_frm, aqi_pm25_nonfrm, aqi_pm25_speciation)
        )) AS max_aqi

    FROM(

        SELECT *
        FROM
        (
            SELECT
                county_code AS county_code_co,
                city_name AS city_name_co,
                longitude AS longitude_co,
                latitude AS latitude_co,
                cbsa_name AS cbsa_name_co,
                site_num AS site_num_co,
                arithmetic_mean AS arithmetic_mean_co,
                aqi AS aqi_co,
                first_max_value AS first_max_value_co,
                first_max_hour AS first_max_hour_co,
                observation_count AS observation_count_co,
                EXTRACT(YEAR FROM date_local) AS year_co,
                EXTRACT(MONTH FROM date_local) AS month_co,
                EXTRACT(DAY FROM date_local) AS day_co,
                EXTRACT(DAYOFWEEK FROM date_local) AS dayofweek_co
            FROM 
                `bigquery-public-data.epa_historical_air_quality.co_daily_summary`
            WHERE 
                state_code = '06'  
            AND 
                EXTRACT(YEAR FROM date_local) IN (
                    2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)
        ) AS CO
        FULL OUTER JOIN 
        (
            SELECT
                county_code AS county_code_no2,
                city_name AS city_name_no2,
                longitude AS longitude_no2,
                latitude AS latitude_no2,
                cbsa_name AS cbsa_name_no2,
                site_num AS site_num_no2,
                arithmetic_mean AS arithmetic_mean_no2,
                aqi AS aqi_no2,
                first_max_value AS first_max_value_no2,
                first_max_hour AS first_max_hour_no2,
                observation_count AS observation_count_no2,
                EXTRACT(YEAR FROM date_local) AS year_no2,
                EXTRACT(MONTH FROM date_local) AS month_no2,
                EXTRACT(DAY FROM date_local) AS day_no2,
                EXTRACT(DAYOFWEEK FROM date_local) AS dayofweek_no2
            FROM 
                `bigquery-public-data.epa_historical_air_quality.no2_daily_summary`
            WHERE 
                state_code = '06'  
            AND 
                EXTRACT(YEAR FROM date_local) IN (
                    2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)
        ) AS NO2
        ON (CO.county_code_co=NO2.county_code_no2 AND CO.city_name_co=NO2.city_name_no2 AND CO.site_num_co=NO2.site_num_no2 AND
            CO.year_co=NO2.year_no2 AND CO.month_co=NO2.month_no2 AND CO.day_co=NO2.day_no2)
        FULL OUTER JOIN 
        (
            SELECT
                county_code AS county_code_o3,
                city_name AS city_name_o3,
                longitude AS longitude_o3,
                latitude AS latitude_o3,
                cbsa_name AS cbsa_name_o3,
                site_num AS site_num_o3,
                arithmetic_mean AS arithmetic_mean_o3,
                aqi AS aqi_o3,
                first_max_value AS first_max_value_o3,
                first_max_hour AS first_max_hour_o3,
                observation_count AS observation_count_o3,
                EXTRACT(YEAR FROM date_local) AS year_o3,
                EXTRACT(MONTH FROM date_local) AS month_o3,
                EXTRACT(DAY FROM date_local) AS day_o3,
                EXTRACT(DAYOFWEEK FROM date_local) AS dayofweek_o3
            FROM 
                `bigquery-public-data.epa_historical_air_quality.o3_daily_summary`
            WHERE 
                state_code = '06'  
            AND 
                EXTRACT(YEAR FROM date_local) IN (
                    2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)
        ) AS O3
        ON (CO.county_code_co=O3.county_code_o3 AND CO.city_name_co=O3.city_name_o3 AND CO.site_num_co=O3.site_num_o3 AND
            CO.year_co=O3.year_o3 AND CO.month_co=O3.month_o3 AND CO.day_co=O3.day_o3)
        FULL OUTER JOIN 
        (
            SELECT
                county_code AS county_code_pm10,
                city_name AS city_name_pm10,
                longitude AS longitude_pm10,
                latitude AS latitude_pm10,
                cbsa_name AS cbsa_name_pm10,
                site_num AS site_num_pm10,
                arithmetic_mean AS arithmetic_mean_pm10,
                aqi AS aqi_pm10,
                first_max_value AS first_max_value_pm10,
                first_max_hour AS first_max_hour_pm10,
                observation_count AS observation_count_pm10,
                EXTRACT(YEAR FROM date_local) AS year_pm10,
                EXTRACT(MONTH FROM date_local) AS month_pm10,
                EXTRACT(DAY FROM date_local) AS day_pm10,
                EXTRACT(DAYOFWEEK FROM date_local) AS dayofweek_pm10
            FROM 
                `bigquery-public-data.epa_historical_air_quality.pm10_daily_summary`
            WHERE 
                state_code = '06'  
            AND 
                EXTRACT(YEAR FROM date_local) IN (
                    2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)
        ) AS PM10
        ON (CO.county_code_co=PM10.county_code_pm10 AND CO.city_name_co=PM10.city_name_pm10 AND CO.site_num_co=PM10.site_num_pm10 AND
            CO.year_co=PM10.year_pm10 AND CO.month_co=PM10.month_pm10 AND CO.day_co=PM10.day_pm10)
        FULL OUTER JOIN 
        (
            SELECT
                county_code AS county_code_pm25_frm,
                city_name AS city_name_pm25_frm,
                longitude AS longitude_pm25_frm,
                latitude AS latitude_pm25_frm,
                cbsa_name AS cbsa_name_pm25_frm,
                site_num AS site_num_pm25_frm,
                arithmetic_mean AS arithmetic_mean_pm25_frm,
                aqi AS aqi_pm25_frm,
                first_max_value AS first_max_value_pm25_frm,
                first_max_hour AS first_max_hour_pm25_frm,
                observation_count AS observation_count_pm25_frm,
                EXTRACT(YEAR FROM date_local) AS year_pm25_frm,
                EXTRACT(MONTH FROM date_local) AS month_pm25_frm,
                EXTRACT(DAY FROM date_local) AS day_pm25_frm,
                EXTRACT(DAYOFWEEK FROM date_local) AS dayofweek_pm25_frm
            FROM 
                `bigquery-public-data.epa_historical_air_quality.pm25_frm_daily_summary`
            WHERE 
                state_code = '06'  
            AND 
                EXTRACT(YEAR FROM date_local) IN (
                    2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)
        ) AS PM25_FRM
        ON (CO.county_code_co=PM25_FRM.county_code_pm25_frm AND CO.city_name_co=PM25_FRM.city_name_pm25_frm AND CO.site_num_co=PM25_FRM.site_num_pm25_frm AND
            CO.year_co=PM25_FRM.year_pm25_frm AND CO.month_co=PM25_FRM.month_pm25_frm AND CO.day_co=PM25_FRM.day_pm25_frm)
        FULL OUTER JOIN 
        (
            SELECT
                county_code AS county_code_pm25_nonfrm,
                city_name AS city_name_pm25_nonfrm,
                longitude AS longitude_pm25_nonfrm,
                latitude AS latitude_pm25_nonfrm, 
                cbsa_name AS cbsa_name_pm25_nonfrm,
                site_num AS site_num_pm25_nonfrm,
                arithmetic_mean AS arithmetic_mean_pm25_nonfrm,
                aqi AS aqi_pm25_nonfrm,
                first_max_value AS first_max_value_pm25_nonfrm,
                first_max_hour AS first_max_hour_p25_nonfrm,
                observation_count AS observation_count_pm25_nonfrm,
                EXTRACT(YEAR FROM date_local) AS year_pm25_nonfrm,
                EXTRACT(MONTH FROM date_local) AS month_pm25_nonfrm,
                EXTRACT(DAY FROM date_local) AS day_pm25_nonfrm,
                EXTRACT(DAYOFWEEK FROM date_local) AS dayofweek_pm25_nonfrm
            FROM 
                `bigquery-public-data.epa_historical_air_quality.pm25_nonfrm_daily_summary`
            WHERE 
                state_code = '06'  
            AND 
                EXTRACT(YEAR FROM date_local) IN (
                    2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)
        ) AS PM25_NONFRM
        ON (CO.county_code_co=PM25_NONFRM.county_code_pm25_nonfrm AND CO.city_name_co=PM25_NONFRM.city_name_pm25_nonfrm AND 
            CO.site_num_co=PM25_NONFRM.site_num_pm25_nonfrm AND CO.year_co=PM25_NONFRM.year_pm25_nonfrm AND 
            CO.month_co=PM25_NONFRM.month_pm25_nonfrm AND CO.day_co=PM25_NONFRM.day_pm25_nonfrm)
        FULL OUTER JOIN 
        (
            SELECT
                county_code AS county_code_pm25_speciation,
                city_name AS city_name_pm25_speciation,
                longitude AS longitude_pm25_speciation,
                latitude AS latitude_pm25_speciation,
                cbsa_name AS cbsa_name_pm25_speciation,
                site_num AS site_num_pm25_speciation,
                arithmetic_mean AS arithmetic_mean_pm25_speciation,
                aqi AS aqi_pm25_speciation,
                first_max_value AS first_max_value_pm25_speciation,
                first_max_hour AS first_max_hour_p25_speciation,
                observation_count AS observation_count_pm25_speciation,
                EXTRACT(YEAR FROM date_local) AS year_pm25_speciation,
                EXTRACT(MONTH FROM date_local) AS month_pm25_speciation,
                EXTRACT(DAY FROM date_local) AS day_pm25_speciation,
                EXTRACT(DAYOFWEEK FROM date_local) AS dayofweek_pm25_speciation
            FROM 
                `bigquery-public-data.epa_historical_air_quality.pm25_speciation_daily_summary`
            WHERE 
                state_code = '06'  
            AND 
                EXTRACT(YEAR FROM date_local) IN (
                    2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)
        ) AS PM25_SPECIATION
        ON (CO.county_code_co=PM25_SPECIATION.county_code_pm25_speciation AND CO.city_name_co=PM25_SPECIATION.city_name_pm25_speciation AND 
            CO.site_num_co=PM25_SPECIATION.site_num_pm25_speciation AND CO.year_co=PM25_SPECIATION.year_pm25_speciation AND 
            CO.month_co=PM25_SPECIATION.month_pm25_speciation AND CO.day_co=PM25_SPECIATION.day_pm25_speciation)
        FULL OUTER JOIN 
        (
            SELECT
                county_code AS county_code_so2,
                city_name AS city_name_so2,
                longitude AS longitude_so2,
                latitude AS latitude_so2,
                cbsa_name AS cbsa_name_so2,
                site_num AS site_num_so2,
                arithmetic_mean AS arithmetic_mean_so2,
                aqi AS aqi_so2,
                first_max_value AS first_max_value_so2,
                first_max_hour AS first_max_hour_so2,
                observation_count AS observation_count_so2,
                EXTRACT(YEAR FROM date_local) AS year_so2,
                EXTRACT(MONTH FROM date_local) AS month_so2,
                EXTRACT(DAY FROM date_local) AS day_so2,
                EXTRACT(DAYOFWEEK FROM date_local) AS dayofweek_so2
            FROM 
                `bigquery-public-data.epa_historical_air_quality.so2_daily_summary`
            WHERE 
                state_code = '06'  
            AND 
                EXTRACT(YEAR FROM date_local) IN (
                    2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)
        ) AS SO2
        ON (CO.county_code_co=SO2.county_code_so2 AND CO.city_name_co=SO2.city_name_so2 AND CO.site_num_co=SO2.site_num_so2 AND
            CO.year_co=SO2.year_so2 AND CO.month_co=SO2.month_so2 AND CO.day_co=SO2.day_so2)
        FULL OUTER JOIN 
        (
            SELECT
                county_code AS county_code_pressure,
                city_name AS city_name_pressure,
                longitude AS longitude_pressure,
                latitude AS latitude_pressure,
                cbsa_name AS cbsa_name_pressure,
                site_num AS site_num_pressure,
                arithmetic_mean AS arithmetic_mean_pressure,
                first_max_value AS first_max_value_pressure,
                first_max_hour AS first_max_hour_pressure,
                observation_count AS observation_count_pressure,
                EXTRACT(YEAR FROM date_local) AS year_pressure,
                EXTRACT(MONTH FROM date_local) AS month_pressure,
                EXTRACT(DAY FROM date_local) AS day_pressure,
                EXTRACT(DAYOFWEEK FROM date_local) AS dayofweek_pressure
            FROM 
                `bigquery-public-data.epa_historical_air_quality.pressure_daily_summary`
            WHERE 
                state_code = '06'  
            AND 
                EXTRACT(YEAR FROM date_local) IN (
                    2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)
        ) AS PRESSURE
        ON (CO.county_code_co=PRESSURE.county_code_pressure AND CO.city_name_co=PRESSURE.city_name_pressure AND CO.site_num_co=PRESSURE.site_num_pressure AND
            CO.year_co=PRESSURE.year_pressure AND CO.month_co=PRESSURE.month_pressure AND CO.day_co=PRESSURE.day_pressure)
        FULL OUTER JOIN 
        (
            SELECT
                county_code AS county_code_wind,
                city_name AS city_name_wind,
                longitude AS longitude_wind,
                latitude AS latitude_wind,
                cbsa_name AS cbsa_name_wind,
                site_num AS site_num_wind,
                arithmetic_mean AS arithmetic_mean_wind,
                first_max_value AS first_max_value_wind,
                first_max_hour AS first_max_hour_wind,
                observation_count AS observation_count_wind,
                EXTRACT(YEAR FROM date_local) AS year_wind,
                EXTRACT(MONTH FROM date_local) AS month_wind,
                EXTRACT(DAY FROM date_local) AS day_wind,
                EXTRACT(DAYOFWEEK FROM date_local) AS dayofweek_wind
            FROM 
                `bigquery-public-data.epa_historical_air_quality.wind_daily_summary`
            WHERE 
                state_code = '06'  
            AND 
                EXTRACT(YEAR FROM date_local) IN (
                    2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)
        ) AS WIND
        ON (CO.county_code_co=WIND.county_code_wind AND CO.city_name_co=WIND.city_name_wind AND CO.site_num_co=WIND.site_num_wind AND
            CO.year_co=WIND.year_wind AND CO.month_co=WIND.month_wind AND CO.day_co=WIND.day_wind)
        FULL OUTER JOIN 
        (
            SELECT
                county_code AS county_code_temp,
                city_name AS city_name_temp,
                longitude AS longitude_temp,
                latitude AS latitude_temp,
                cbsa_name AS cbsa_name_temp,
                site_num AS site_num_temp,
                arithmetic_mean AS arithmetic_mean_temp,
                first_max_value AS first_max_value_temp,
                first_max_hour AS first_max_hour_temp,
                observation_count AS observation_count_temp,
                EXTRACT(YEAR FROM date_local) AS year_temp,
                EXTRACT(MONTH FROM date_local) AS month_temp,
                EXTRACT(DAY FROM date_local) AS day_temp,
                EXTRACT(DAYOFWEEK FROM date_local) AS dayofweek_temp
            FROM 
                `bigquery-public-data.epa_historical_air_quality.temperature_daily_summary`
            WHERE 
                state_code = '06'  
            AND 
                EXTRACT(YEAR FROM date_local) IN (
                    2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017)
        ) AS TEMP
        ON (CO.county_code_co=TEMP.county_code_temp AND CO.city_name_co=TEMP.city_name_temp AND CO.site_num_co=TEMP.site_num_temp AND
            CO.year_co=TEMP.year_temp AND CO.month_co=TEMP.month_temp AND CO.day_co=TEMP.day_temp)
    ) AS innerquery

    GROUP BY 1,2,3,4,5
    ORDER BY 1,2,3,4,5 ASC
) AS outterquery;