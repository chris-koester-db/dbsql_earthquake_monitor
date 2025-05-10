-- Databricks notebook source
-- MAGIC %md
-- MAGIC An [HTTP connection](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-connection) must be created before using the functions in this solution.

-- COMMAND ----------

create connection if not exists databricks_api
type HTTP
options (
  host 'https://cust-success.cloud.databricks.com',
  port '443',
  base_path '/api/',
  bearer_token 'your_token' -- Personal access token
);
