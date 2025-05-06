-- Databricks notebook source
-- MAGIC %md
-- MAGIC Beta solution showing how to page through API results sequentially, which is a common pattern when returning a large number of results from an API.
-- MAGIC
-- MAGIC Notebook Widgets:
-- MAGIC - start_time and end_time must use the format `2025-04-28 15:40:49`
-- MAGIC - warehouse_ids can be provided as a single value (4b9b953939869799) or a comma separated list (4b9b953939869799,4b9b953939869800)

-- COMMAND ----------

use catalog identifier(:catalog);
use schema identifier(:schema);

-- COMMAND ----------

begin
  declare start_time bigint;
  declare end_time bigint;
  declare resp struct<has_next_page: boolean, next_page_token: string, res: array<struct<channel_used: struct<dbsql_version: string, name: string>, client_application: string, duration: bigint, endpoint_id: string, error_message: string, executed_as_user_id: bigint, executed_as_user_name: string, execution_end_time_ms: bigint, is_final: boolean, lookup_key: string, metrics: struct<compilation_time_ms: bigint, execution_time_ms: bigint, network_sent_bytes: bigint, overloading_queue_start_timestamp: bigint, photon_total_time_ms: bigint, provisioning_queue_start_timestamp: bigint, pruned_bytes: bigint, pruned_files_count: bigint, query_compilation_start_timestamp: bigint, read_bytes: bigint, read_cache_bytes: bigint, read_files_count: bigint, read_partitions_count: bigint, read_remote_bytes: bigint, result_fetch_time_ms: bigint, result_from_cache: boolean, rows_produced_count: bigint, rows_read_count: bigint, spill_to_disk_bytes: bigint, task_total_time_ms: bigint, total_time_ms: bigint, write_remote_bytes: bigint>, plans_state: string, query_end_time_ms: bigint, query_id: string, query_source: struct<alert_id: string, dashboard_id: string, genie_space_id: string, job_info: struct<job_id: string, job_run_id: string, job_task_run_id: string>, legacy_dashboard_id: string, notebook_id: string, sql_query_id: string>, query_start_time_ms: bigint, query_text: string, rows_produced: bigint, spark_ui_url: string, statement_type: string, status: string, user_id: bigint, user_name: string, warehouse_id: string>>>;
  
  -- TODO: determine how to remove redundant schema
  declare res_array array<struct<channel_used:struct<dbsql_version:string,name:string>,client_application:string,duration:bigint,endpoint_id:string,error_message:string,executed_as_user_id:bigint,executed_as_user_name:string,execution_end_time_ms:bigint,is_final:boolean,lookup_key:string,metrics:struct<compilation_time_ms:bigint,execution_time_ms:bigint,network_sent_bytes:bigint,overloading_queue_start_timestamp:bigint,photon_total_time_ms:bigint,provisioning_queue_start_timestamp:bigint,pruned_bytes:bigint,pruned_files_count:bigint,query_compilation_start_timestamp:bigint,read_bytes:bigint,read_cache_bytes:bigint,read_files_count:bigint,read_partitions_count:bigint,read_remote_bytes:bigint,result_fetch_time_ms:bigint,result_from_cache:boolean,rows_produced_count:bigint,rows_read_count:bigint,spill_to_disk_bytes:bigint,task_total_time_ms:bigint,total_time_ms:bigint,write_remote_bytes:bigint>,plans_state:string,query_end_time_ms:bigint,query_id:string,query_source:struct<alert_id:string,dashboard_id:string,genie_space_id:string,job_info:struct<job_id:string,job_run_id:string,job_task_run_id:string>,legacy_dashboard_id:string,notebook_id:string,sql_query_id:string>,query_start_time_ms:bigint,query_text:string,rows_produced:bigint,spark_ui_url:string,statement_type:string,status:string,user_id:bigint,user_name:string,warehouse_id:string>>;
  
  declare page_token string;
  declare arg_map map<string, string>;
  
  set start_time = (select unix_timestamp_ms(:start_time));
  set end_time = (select unix_timestamp_ms(:end_time));
  
  -- Get first page of results
  set resp = (
    select list_queries(
      start_time_ms => start_time,
      end_time_ms => end_time,
      warehouse_ids => split(:warehouse_ids, ','),
      max_results => 1000,
      page_token => null,
      include_metrics => true
    )
  );
  
  -- Add results to res_array variable
  set res_array = resp.res;
  
  -- Set token for next page of results. Will be null of not present.
  set page_token = resp.next_page_token;
  
  -- Get remaining pages of results
  get_qry_hist: while page_token is not null do
    set resp = (
      select list_queries(
        start_time_ms => start_time,
        end_time_ms => end_time,
        warehouse_ids => split(:warehouse_ids, ','),
        max_results => 1000,
        page_token => page_token,
        include_metrics => true
      )
    );

    set res_array = array_union(res_array, resp.res);

    -- Set token for next page of results. Will be null of not present.
    set page_token = resp.next_page_token;
  end while;
  
  -- Explode array of results and select desired columns
  -- Filter out placeholder used to infer schema
  from (select explode(res_array))
  |> select
       col.query_id,
       col.status,
       col.query_text,
       col.query_start_time_ms,
       col.execution_end_time_ms,
       col.query_end_time_ms,
       col.user_id,
       col.user_name,
       col.spark_ui_url,
       col.warehouse_id,
       col.error_message,
       col.rows_produced,
       col.metrics,
       col.is_final,
       col.channel_used,
       col.duration,
       col.executed_as_user_id,
       col.executed_as_user_name,
       col.plans_state,
       col.statement_type;

end;