/* DISCLAIMER: Please replace <your-amazon-redshift-sagemaker-iam-role-arn> with the IAM role ARN of your Amazon Redshift cluster in the SQL scripts below


/* Data Preparation */

DROP TABLE IF EXISTS ridership CASCADE;
DROP TABLE IF EXISTS weather CASCADE;
DROP TABLE IF EXISTS holiday CASCADE;
DROP TABLE IF EXISTS trip_data CASCADE;

CREATE TABLE IF NOT EXISTS ridership
( trip_id               INT
, trip_duration_seconds INT
, trip_start_time       timestamp
, trip_stop_time        timestamp
, from_station_name     VARCHAR(50)
, to_station_name       VARCHAR(50)
, from_station_id       SMALLINT
, to_station_id         SMALLINT
, user_type             VARCHAR(20));

CREATE TABLE IF NOT EXISTS weather
( longitude_x         DECIMAL(5,2)
, latitude_y          DECIMAL(5,2)
, station_name        VARCHAR(20)
, climate_id          BIGINT
, datetime_utc        TIMESTAMP
, weather_year        SMALLINT
, weather_month       SMALLINT
, weather_day         SMALLINT
, time_utc            VARCHAR(5)
, temp_c              DECIMAL(5,2)
, temp_flag           VARCHAR(1)
, dew_point_temp_c    DECIMAL(5,2)
, dew_point_temp_flag VARCHAR(1)
, rel_hum             SMALLINT
, rel_hum_flag        VARCHAR(1)
, precip_amount_mm    DECIMAL(5,2)
, precip_amount_flag  VARCHAR(1)
, wind_dir_10s_deg    VARCHAR(10)
, wind_dir_flag       VARCHAR(1)
, wind_spd_kmh        VARCHAR(10)
, wind_spd_flag       VARCHAR(1)
, visibility_km       VARCHAR(10)
, visibility_flag     VARCHAR(1)
, stn_press_kpa       DECIMAL(5,2)
, stn_press_flag      VARCHAR(1)
, hmdx                SMALLINT
, hmdx_flag           VARCHAR(1)
, wind_chill          VARCHAR(10)
, wind_chill_flag     VARCHAR(1)
, weather             VARCHAR(10));

CREATE TABLE IF NOT EXISTS holiday
( holiday_date  DATE
, description VARCHAR(100));


COPY ridership FROM 
's3://redshift-ml-bikesharing-data/bike-sharing-data/ridership/'
IAM_ROLE '<your-amazon-redshift-sagemaker-iam-role-arn>'
FORMAT csv IGNOREHEADER 1 DATEFORMAT 'auto' TIMEFORMAT 'auto' REGION 'us-west-2' gzip;

COPY weather FROM
's3://redshift-ml-bikesharing-data/bike-sharing-data/weather/'
IAM_ROLE '<your-amazon-redshift-sagemaker-iam-role-arn>'
FORMAT csv IGNOREHEADER 1 DATEFORMAT 'auto' TIMEFORMAT 'auto' REGION 'us-west-2' gzip;

COPY holiday FROM
's3://redshift-ml-bikesharing-data/bike-sharing-data/holiday/'
IAM_ROLE '<your-amazon-redshift-sagemaker-iam-role-arn>'
FORMAT csv IGNOREHEADER 1 DATEFORMAT 'auto' TIMEFORMAT 'auto' REGION 'us-west-2' gzip;

CREATE OR REPLACE VIEW ridership_view AS
SELECT
    trip_time
    , trip_count
    , TO_CHAR(trip_time,'hh24') ::INT trip_hour
    , TO_CHAR(trip_time, 'dd') :: INT trip_day
    , TO_CHAR(trip_time, 'mm') :: INT trip_month
    , TO_CHAR(trip_time, 'yy') :: INT trip_year
    , TO_CHAR(trip_time, 'q') :: INT trip_quarter
    , TO_CHAR(trip_time, 'w') :: INT trip_month_week
    , TO_CHAR(trip_time, 'd') :: INT trip_week_day
FROM  
    (SELECT  
         CASE
           WHEN TRUNC(r.trip_start_time) < '2017-07-01'::DATE
           THEN CONVERT_TIMEZONE('US/Eastern', DATE_TRUNC('hour',r.trip_start_time))
           ELSE DATE_TRUNC('hour',r.trip_start_time)
         END trip_time
         , COUNT(1) trip_count
     FROM    
         ridership r
     WHERE    r.trip_duration_seconds BETWEEN 60 AND 60 * 60 * 24
     GROUP BY
         1);

CREATE OR REPLACE VIEW weather_view AS
SELECT  
    CONVERT_TIMEZONE('US/Eastern', 
      DATE_TRUNC('hour',datetime_utc)) daytime
    , ROUND(AVG(temp_c)) temp_c
    , ROUND(AVG(precip_amount_mm)) precip_amount_mm
FROM weather
GROUP BY 1;

DROP TABLE IF EXISTS trip_data;
CREATE TABLE trip_data AS 
SELECT         
   r.trip_time
  ,r.trip_count
  ,r.trip_hour
  ,r.trip_day
  ,r.trip_month
  ,r.trip_year
  ,r.trip_quarter
  ,r.trip_month_week
  ,r.trip_week_day
  ,w.temp_c
  ,w.precip_amount_mm
  ,CASE
      WHEN h.holiday_date IS NOT NULL
      THEN 1
      WHEN TO_CHAR(r.trip_time,'D')::INT IN (1,7)
      THEN 1
      ELSE 0
    END is_holiday
  , ROW_NUMBER() OVER (ORDER BY RANDOM()) serial_number
FROM           
  ridership_view r
JOIN            weather_view w
  ON ( r.trip_time = w.daytime )
LEFT OUTER JOIN holiday h
  ON ( TRUNC(r.trip_time) = h.holiday_date );