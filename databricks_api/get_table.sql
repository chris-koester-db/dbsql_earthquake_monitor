-- Databricks notebook source
use catalog identifier(:catalog);
use schema identifier(:schema);

-- COMMAND ----------

describe function extended get_table

-- COMMAND ----------

from (select get_table('tpcds.sf_10000_liquid.catalog_returns') as resp_variant)
|> select explode(variant_get(resp_variant, '$.columns')::array<variant>)
|> select
     col:name::string AS column_name,
     col:type_name::string AS data_type,
     col:nullable::boolean AS is_nullable