NYC_TRIPS.STAGE_STAGENYC_TRIPS.STAGE_STAGEUSE ROLE accountadmin;

-- create a role 'transform' for data transformation and assign accountadmin to it
CREATE ROLE IF NOT EXISTS transform;
GRANT ROLE transform TO ROLE accountadmin;
-- create a defualt warehouse for executions
CREATE WAREHOUSE IF NOT EXISTS compute_wh;
GRANT OPERATE ON WAREHOUSE compute_wh TO ROLE transform;

-- create a user 'dbt' to own all the data transformations
CREATE USER IF NOT EXISTS dbt
    PASSWORD='s'
    LOGIN_NAME='dbt'
    MUST_CHANGE_PASSWORD=FALSE
    DEFAULT_ROLE='transform'
    DEFAULT_WAREHOUSE='compute_wh'
    DEFAULT_NAMESPACE='NYC_TRIPS'
    COMMENT='DBT user used for data transformation';

ALTER USER dbt SET TYPE = LEGACY_SERVICE;
GRANT ROLE transform TO USER dbt;

CREATE DATABASE IF NOT EXISTS NYC_TRIPS;
CREATE SCHEMA IF NOT EXISTS NYC_TRIPS.RAW;

-- grant all premission for this role 'transform'
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;
GRANT ALL ON DATABASE NYC_TRIPS TO ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE NYC_TRIPS TO ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE NYC_TRIPS TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA NYC_TRIPS.RAW TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA NYC_TRIPS.RAW TO ROLE TRANSFORM;
GRANT ALL ON ALL STAGES IN DATABASE NYC_TRIPS TO ROLE TRANSFORM;

-- create a stage to conncet to s3 to load data from it
CREATE STAGE IF NOT EXISTS NYC_TRIPS_STAGE
    URL='S3://newyorkcitytrips2009' -- bucket name
    CREDENTIALS=(AWS_KEY_ID='' AWS_SECRET_KEY=''); --credentials of uesr 

USE DATABASE NYC_TRIPS;
USE SCHEMA NYC_TRIPS.RAW;

DROP TABLE RAW.RAW_TRIPS;

-- create table for dataset trips
CREATE OR REPLACE TABLE RAW.RAW_TRIPS (
    vendor_name STRING,
    trip_pickup_datetime TIMESTAMPNTZ,
    trip_dropoff_datetime TIMESTAMPNTZ,
    passenger_count SMALLINT,
    trip_distance FLOAT,
    start_lon FLOAT,
    start_lat FLOAT,
    rate_code TINYINT,
    store_and_forward FLOAT,
    end_lon FLOAT,
    end_lat FLOAT,
    payment_type STRING,
    fare_amt FLOAT,
    surcharge FLOAT,
    mta_tax FLOAT,
    tip_amt FLOAT,
    tolls_amt FLOAT,
    total_amt FLOAT,
    start_zone STRING,
    start_borough STRING,
    end_zone STRING,
    end_borough STRING
    

);

COPY INTO RAW.RAW_TRIPS
FROM '@NYC_TRIPS_STAGE/yellow_tripdata_2009-01_enriched.parquet'
FILE_FORMAT = (TYPE = 'parquet' COMPRESSION = AUTO)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';

COPY INTO RAW.RAW_TRIPS
FROM '@NYC_TRIPS_STAGE/yellow_tripdata_2009-02_enriched2.parquet'
FILE_FORMAT = (TYPE = 'parquet' COMPRESSION = AUTO)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';






-- delete duplicates in stage.stg_trips
DELETE FROM _stage.stg_trips
WHERE trip_id IN (
    SELECT trip_id
    FROM (
        SELECT trip_id,
               ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY trip_pickup_datetime) AS rn
        FROM _stage.stg_trips
    )
    WHERE rn > 1
);









    




