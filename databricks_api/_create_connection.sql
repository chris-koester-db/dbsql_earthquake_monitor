-- Databricks notebook source
create connection if not exists databricks_api
type HTTP
options (
  host 'https://cust-success.cloud.databricks.com',
  port '443',
  base_path '/api/',
  bearer_token 'your_token' -- Personal access token
);