-- Databricks notebook source
use catalog identifier(:catalog);
use schema identifier(:schema);

-- COMMAND ----------

describe function extended create_a_warehouse

-- COMMAND ----------

select create_a_warehouse(
  name => :warehouse_name,
  cluster_size => :cluster_size,
  max_num_clusters => :max_num_clusters,
  auto_stop_mins => :auto_stop_mins
) as resp