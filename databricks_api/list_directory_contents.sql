-- Databricks notebook source
-- DBTITLE 1,List Contents of Volume
with resp as (
select
  parse_json(
    http_request(
      conn => 'databricks_api',
      method => 'GET',
      path => '2.0/fs/directories/Volumes/catalog/schema/volume'
    ).text
  ) as resp_variant
)
-- Convert array of features to rows
,response_rows as (
  select
    explode(variant_get(resp_variant, '$.contents')::array<variant>) as col
  from resp
)
-- Select desired columns
select
  col:file_size,
  col:is_directory,
  col:name,
  col:path
from response_rows