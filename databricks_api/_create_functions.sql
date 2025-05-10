-- Databricks notebook source
use catalog identifier(:catalog);
use schema identifier(:schema);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Unity Catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tables

-- COMMAND ----------

-- DBTITLE 1,Get a Table
-- https://docs.databricks.com/api/workspace/tables/get
-- create function if not exists get_table(table_name string)
create or replace function get_table(table_name string)
comment 'Gets a table from the metastore for a specific catalog and schema. https://docs.databricks.com/api/workspace/tables/get'
return
from (select
  http_request(
    conn => 'databricks_api',
    method => 'GET',
    path => concat('2.1/unity-catalog/tables/', table_name)
  ).text as resp
)
|> select
     from_json(
       resp,
       'STRUCT<access_point: STRING, browse_only: BOOLEAN, catalog_name: STRING, columns: ARRAY<STRUCT<comment: STRING, mask: STRUCT<function_name: STRING, using_column_names: ARRAY<STRING>>, name: STRING, nullable: BOOLEAN, partition_index: BIGINT, position: BIGINT, type_interval_type: STRING, type_json: STRING, type_name: STRING, type_precision: BIGINT, type_scale: BIGINT, type_text: STRING>>, comment: STRING, created_at: BIGINT, created_by: STRING, data_access_configuration_id: STRING, data_source_format: STRING, deleted_at: BIGINT, delta_runtime_properties_kvpairs: STRUCT<delta_runtime_properties: STRUCT<property1: STRING, property2: STRING>>, effective_predictive_optimization_flag: STRUCT<inherited_from_name: STRING, inherited_from_type: STRING, value: STRING>, enable_predictive_optimization: STRING, full_name: STRING, metastore_id: STRING, name: STRING, owner: STRING, pipeline_id: STRING, properties: STRUCT<property1: STRING, property2: STRING>, row_filter: STRUCT<function_name: STRING, input_column_names: ARRAY<STRING>>, schema_name: STRING, sql_path: STRING, storage_credential_name: STRING, storage_location: STRING, table_constraints: ARRAY<STRUCT<foreign_key_constraint: STRUCT<child_columns: ARRAY<STRING>, name: STRING, parent_columns: ARRAY<STRING>, parent_table: STRING>, named_table_constraint: STRUCT<name: STRING>, primary_key_constraint: STRUCT<child_columns: ARRAY<STRING>, name: STRING>>>, table_id: STRING, table_type: STRING, updated_at: BIGINT, updated_by: STRING, view_definition: STRING, view_dependencies: STRUCT<dependencies: ARRAY<STRUCT<function: STRUCT<function_full_name: STRING>, table: STRUCT<table_full_name: STRING>>>>>'
     ) as resp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # File Management

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Files

-- COMMAND ----------

-- DBTITLE 1,List Directory Contents
-- https://docs.databricks.com/api/workspace/files/listdirectorycontents
create or replace function list_directory_contents(directory_path string)
comment 'Returns the contents of a directory. If there is no directory at the specified path, the API returns a HTTP 404 error. https://docs.databricks.com/api/workspace/files/listdirectorycontents'
return
from (select
  http_request(
    conn => 'databricks_api',
    method => 'GET',
    path => concat('2.0/fs/directories', directory_path)
  ).text as resp
)
|> select
     from_json(
       resp,
       'STRUCT<contents: ARRAY<STRUCT<file_size: BIGINT, is_directory: BOOLEAN, last_modified: BIGINT, name: STRING, path: STRING>>>'
     ) as resp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Databricks SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SQL Warehouses

-- COMMAND ----------

-- DBTITLE 1,List Warehouses
-- https://docs.databricks.com/api/workspace/warehouses/list
create or replace function list_warehouses(
  run_as_user_id int default null
)
comment 'Lists all SQL warehouses that a user has manager permissions on. https://docs.databricks.com/api/workspace/warehouses/list'
return
from (select
  http_request(
    conn => 'databricks_api',
    method => 'GET',
    path => '2.0/sql/warehouses',
    json => 
      to_json(
        named_struct(
          'run_as_user_id', run_as_user_id
        )
      )
  ).text as resp
)
|> select
     from_json(
       resp,
       'STRUCT<warehouses: ARRAY<STRUCT<auto_stop_mins: STRING, channel: STRUCT<dbsql_version: STRING, name: STRING>, cluster_size: STRING, creator_name: STRING, enable_photon: BOOLEAN, enable_serverless_compute: BOOLEAN, health: STRUCT<details: STRING, failure_reason: STRUCT<code: STRING, parameters: STRUCT<property1: STRING, property2: STRING>, type: STRING>, message: STRING, status: STRING, summary: STRING>, id: STRING, instance_profile_arn: STRING, jdbc_url: STRING, max_num_clusters: BIGINT, min_num_clusters: STRING, name: STRING, num_active_sessions: BIGINT, num_clusters: BIGINT, odbc_params: STRUCT<hostname: STRING, path: STRING, port: BIGINT, protocol: STRING>, spot_instance_policy: STRING, state: STRING, tags: STRUCT<custom_tags: ARRAY<STRUCT<key: STRING, value: STRING>>>, warehouse_type: STRING>>>'
     ) as resp;

-- COMMAND ----------

-- DBTITLE 1,Create a Warehouse
-- https://docs.databricks.com/api/workspace/warehouses/create
create or replace function create_a_warehouse(
  name string,
  cluster_size string,
  max_num_clusters int,
  auto_stop_mins int
)
comment 'Creates a new SQL warehouse. https://docs.databricks.com/api/workspace/warehouses/create'
return
from (select
  http_request(
    conn => 'databricks_api',
    method => 'POST',
    path => '2.0/sql/warehouses',
    json => 
      to_json(
        named_struct(
          'name', name,
          'cluster_size', cluster_size,
          'max_num_clusters', max_num_clusters,
          'auto_stop_mins', auto_stop_mins
        )
      )
  ).text as resp
)
|> select
     from_json(
       resp,
       'STRUCT<id: STRING>'
     ) as resp;

-- COMMAND ----------

-- DBTITLE 1,Get Warehouse Info
-- https://docs.databricks.com/api/workspace/warehouses/get
create or replace function get_warehouse_info(
  id string
)
comment 'Gets the information for a single SQL warehouse. https://docs.databricks.com/api/workspace/warehouses/get'
return
from (select
  http_request(
    conn => 'databricks_api',
    method => 'GET',
    path => concat('2.0/sql/warehouses/', id)
  ).text as resp
)
|> select
     from_json(
       resp,
       'STRUCT<auto_stop_mins: STRING, channel: STRUCT<dbsql_version: STRING, name: STRING>, cluster_size: STRING, creator_name: STRING, enable_photon: BOOLEAN, enable_serverless_compute: BOOLEAN, health: STRUCT<details: STRING, failure_reason: STRUCT<code: STRING, parameters: STRUCT<property1: STRING, property2: STRING>, type: STRING>, message: STRING, status: STRING, summary: STRING>, id: STRING, instance_profile_arn: STRING, jdbc_url: STRING, max_num_clusters: BIGINT, min_num_clusters: STRING, name: STRING, num_active_sessions: BIGINT, num_clusters: BIGINT, odbc_params: STRUCT<hostname: STRING, path: STRING, port: BIGINT, protocol: STRING>, spot_instance_policy: STRING, state: STRING, tags: STRUCT<custom_tags: ARRAY<STRUCT<key: STRING, value: STRING>>>, warehouse_type: STRING>'
     ) as resp;

-- COMMAND ----------

-- DBTITLE 1,Delete a Warehouse
-- https://docs.databricks.com/api/workspace/warehouses/delete
create or replace function delete_warehouse(
  id string
)
comment 'Deletes a SQL warehouse. https://docs.databricks.com/api/workspace/warehouses/delete'
return
select
  http_request(
    conn => 'databricks_api',
    method => 'DELETE',
    path => concat('2.0/sql/warehouses/', id)
  ).text as resp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query History

-- COMMAND ----------

-- DBTITLE 1,List Queries
-- A python function is used only so that this solution can be reconciled using
-- the Databricks SDK. Python and SQL functions don't return identical Unix timestamps.
create or replace function unix_timestamp_ms(dt string)
  returns bigint
  deterministic
  comment 'Returns the number of milliseconds since the Unix Epoch'
  language python
  as $$
    from datetime import datetime
    dt_object = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
    return int(dt_object.timestamp() * 1000)
  $$;

-- https://docs.databricks.com/api/workspace/queryhistory/list
-- include_metrics type is string because of a bug related to boolean arguments
create or replace function list_queries(
  start_time_ms bigint,
  end_time_ms bigint,
  warehouse_ids array<string>,
  page_token string,
  max_results int default 100,
  include_metrics string default 'true'
)
comment 'List the history of queries through SQL warehouses, and serverless compute. You can filter by user ID, warehouse ID, status, and time range. Most recently started queries are returned first (up to max_results in request). The pagination token returned in response can be used to list subsequent query statuses. https://docs.databricks.com/api/workspace/queryhistory/list'
return
from (select
  http_request(
    conn => 'databricks_api',
    method => 'GET',
    path => '2.0/sql/history/queries',
    json => 
      to_json(
        named_struct('filter_by',
          named_struct('query_start_time_range',
            named_struct('start_time_ms', start_time_ms, 'end_time_ms', end_time_ms),
            'warehouse_ids', warehouse_ids
          ),
          'max_results', max_results,
          'page_token', page_token,
          'include_metrics', include_metrics
        )
      )
  ).text as resp
)
|> select
     from_json(
       resp,
       'struct<has_next_page: boolean, next_page_token: string, res: array<struct<channel_used: struct<dbsql_version: string, name: string>, client_application: string, duration: bigint, endpoint_id: string, error_message: string, executed_as_user_id: bigint, executed_as_user_name: string, execution_end_time_ms: bigint, is_final: boolean, lookup_key: string, metrics: struct<compilation_time_ms: bigint, execution_time_ms: bigint, network_sent_bytes: bigint, overloading_queue_start_timestamp: bigint, photon_total_time_ms: bigint, provisioning_queue_start_timestamp: bigint, pruned_bytes: bigint, pruned_files_count: bigint, query_compilation_start_timestamp: bigint, read_bytes: bigint, read_cache_bytes: bigint, read_files_count: bigint, read_partitions_count: bigint, read_remote_bytes: bigint, result_fetch_time_ms: bigint, result_from_cache: boolean, rows_produced_count: bigint, rows_read_count: bigint, spill_to_disk_bytes: bigint, task_total_time_ms: bigint, total_time_ms: bigint, write_remote_bytes: bigint>, plans_state: string, query_end_time_ms: bigint, query_id: string, query_source: struct<alert_id: string, dashboard_id: string, genie_space_id: string, job_info: struct<job_id: string, job_run_id: string, job_task_run_id: string>, legacy_dashboard_id: string, notebook_id: string, sql_query_id: string>, query_start_time_ms: bigint, query_text: string, rows_produced: bigint, spark_ui_url: string, statement_type: string, status: string, user_id: bigint, user_name: string, warehouse_id: string>>>'
     );
