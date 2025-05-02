-- Databricks notebook source
-- Function that returns response of "Get a table" API
-- https://docs.databricks.com/api/workspace/tables/get
create or replace temporary function get_table(table_name string) return
select
  parse_json(
    http_request(
      conn => 'databricks_api',
      method => 'GET',
      path => concat('2.1/unity-catalog/tables/', table_name)
    ).text
  ) as resp_variant;

-- COMMAND ----------

-- Call the function
select get_table('tpcds.sf_10000_liquid.catalog_returns') as table_details

-- COMMAND ----------

-- Parse column details from response
with resp as (
select get_table('tpcds.sf_10000_liquid.catalog_returns') as resp_variant
)
-- Convert array of features to rows
,response_rows as (
  select
    explode(variant_get(resp_variant, '$.columns')::array<variant>) as col
  from resp
)
-- Select desired columns
select
  col:name::string AS column_name,
  col:type_name::string AS data_type,
  col:nullable::boolean AS is_nullable
from response_rows