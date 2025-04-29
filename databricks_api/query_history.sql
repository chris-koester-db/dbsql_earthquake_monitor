-- Databricks notebook source
-- MAGIC %md
-- MAGIC beta solution showing how to page through API results sequentially, which is a common pattern when returning a large number of results from an API.

-- COMMAND ----------

-- DBTITLE 1,Function to Get Query History
create or replace temporary function get_query_history(
  start_time_ms bigint,
  end_time_ms bigint,
  warehouse_ids array<string>,
  page_token string,
  max_results int default 100,
  include_metrics boolean default true
) return
select
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
  ) as resp;

-- COMMAND ----------

-- DBTITLE 1,Function to Get Unix Timestamp
-- A python function is used only so that this solution can be reconciled using
-- the Databricks SDK. Python and SQL functions don't return identical Unix timestamps.
create or replace function main.chris_koester.unix_timestamp_ms(dt string)
  returns bigint
  language python
  as $$
    from datetime import datetime
    dt_object = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
    return int(dt_object.timestamp() * 1000)
  $$

-- COMMAND ----------

begin
  declare start_time bigint;
  declare end_time bigint;
  declare resp struct<status_code int, text string>;
  
  -- Sample json used to derive schema for both res_array variable and query history response
  declare json_str string = '{"has_next_page":true,"next_page_token":"Ci0KJDU4NjEwZjY5LTgzNzUtNDdiMS04YTg1LWYxNTU5ODI5MDYyMhDdobu","res":[{"channel_used":{"dbsql_version":"2022.30","name":"CHANNEL_NAME_PREVIEW"},"client_application":"Power BI","duration":1000,"endpoint_id":"string","error_message":"Table or view not found: customers;","executed_as_user_id":0,"executed_as_user_name":"string","execution_end_time_ms":1595357086373,"is_final":true,"lookup_key":"CiQ3OGFkYmQ2Zi00ZGUwLTRlNTYtOTkxZC05Y2I5OTNlZTViYjcQ4N6r/dguGhBlM2VlYTVlOTExMjFkMzNjILPbh9OK6uoL","metrics":{"compilation_time_ms":0,"execution_time_ms":0,"network_sent_bytes":0,"overloading_queue_start_timestamp":0,"photon_total_time_ms":0,"provisioning_queue_start_timestamp":0,"pruned_bytes":0,"pruned_files_count":0,"query_compilation_start_timestamp":0,"read_bytes":0,"read_cache_bytes":0,"read_files_count":0,"read_partitions_count":0,"read_remote_bytes":0,"result_fetch_time_ms":0,"result_from_cache":true,"rows_produced_count":0,"rows_read_count":0,"spill_to_disk_bytes":0,"task_total_time_ms":0,"total_time_ms":0,"write_remote_bytes":0},"plans_state":"IGNORED_SMALL_DURATION","query_end_time_ms":1595357087200,"query_id":"00000000-0000-0000-0000-000000000000","query_source":{"alert_id":"d789836c-56ef-4c89-b951-d7186c4ad3ee","dashboard_id":"b1efe7f5891c1815b65e21c873fdaf4e","genie_space_id":"a1f008dd4daf1340a7d59c66c2bdc5a8","job_info":{"job_id":"445923364221868","job_run_id":"string","job_task_run_id":"870588346649939"},"legacy_dashboard_id":"caf1e170-d14e-4bcc-8019-8c3b26ca46e4","notebook_id":"1335125300829196","sql_query_id":"be6df0a0-c317-44df-9659-b4f206c5d027"},"query_start_time_ms":1595357086200,"query_text":"SELECT * FROM customers;","rows_produced":100,"spark_ui_url":"https://<databricks-instance>/sparkui/1234-567890-test123/driver-1234567890123456789/SQL/execution/?id=0","statement_type":"OTHER","status":"QUEUED","user_id":1234567890123456,"user_name":"user@example.com","warehouse_id":"098765321fedcba"}]}';
  
  declare res_array = array(from_json(json_str, schema_of_json(json_str)).res[0]);
  declare page_token string;
  declare arg_map map<string, string>;

  set start_time = (select main.chris_koester.unix_timestamp_ms(:start_time));
  set end_time = (select main.chris_koester.unix_timestamp_ms(:end_time));
  
  -- Get first page of results
  set resp = (
    select get_query_history(
      start_time_ms => start_time,
      end_time_ms => end_time,
      warehouse_ids => array('4b9b953939869799'),
      max_results => 1000,
      page_token => null,
      include_metrics => true
    )
  );
  
  -- Add results to res_array variable
  set res_array = array_union(
    res_array, 
    (select from_json(resp.text, schema_of_json(json_str)).res)
  );
  
  -- Set token for next page of results. Will be null if not present.
  set page_token = (select from_json(resp.text, schema_of_json(json_str)).next_page_token);
  
  -- Get remaining pages of results
  get_qry_hist: while page_token is not null do
    set resp = (
      select get_query_history(
        start_time_ms => start_time,
        end_time_ms => end_time,
        warehouse_ids => array('4b9b953939869799'),
        max_results => 1000,
        page_token => page_token,
        include_metrics => true
      )
    );

    set res_array = array_union(
      res_array, 
      (select from_json(resp.text, schema_of_json(json_str)).res)
    );

    -- Set token for next page of results. Will be null if not present.
    set page_token = (select from_json(resp.text, schema_of_json(json_str)).next_page_token);
  end while;
  
  -- Explode array of results and select desired columns
  -- Filter out placeholder used to infer schema
  with response_rows as (
    select explode(res_array)
  )
  select
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
    col.statement_type
  from response_rows
  where col.query_id != '00000000-0000-0000-0000-000000000000';

end;