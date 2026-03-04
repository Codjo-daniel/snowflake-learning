-- Setting up database and schema for the exercice
create database learning;
use learning;
create schema snowflake;
use schema snowflake;

-- Setting up the table for the json file
create or replace table raw_source(
    src variant
);

-- Setting up an external stage
create or replace stage json_basic_stage
url = 's3://snowflake-docs/tutorials/json'

-- load data
copy into raw_source
  FROM @json_basic_stage/server/2.6/2016/07/15/15
  file_format = (type = json);

-- Query the raw table
select * from raw_source

-- query json data
select
    src:device_type::string as device_type,
    src:events as events
from raw_source

-- flatten the array
create or replace table curated_source as
select
  src:device_type::string as device_type,
  src:version::float as version,
  value as src
  from
    raw_source
  , lateral flatten( input => src:events);

-- Query the newly created table:
select * from curated_source