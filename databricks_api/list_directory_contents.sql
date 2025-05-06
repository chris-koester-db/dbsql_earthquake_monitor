-- Databricks notebook source
use catalog identifier(:catalog);
use schema identifier(:schema);

-- COMMAND ----------

describe function extended list_directory_contents

-- COMMAND ----------

from (select list_directory_contents(:directory_path) as resp)
|> select explode(resp.contents) as col
|> select
     col.file_size,
     col.is_directory,
     col.name,
     col.path