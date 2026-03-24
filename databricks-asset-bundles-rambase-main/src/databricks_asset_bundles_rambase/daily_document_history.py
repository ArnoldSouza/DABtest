# Databricks notebook source
"""Databricks notebook for daily document history."""

# COMMAND ----------
# MAGIC %md
# MAGIC # Environment variables Dev/Prod

# COMMAND ----------
external_rambase_catalog_name = dbutils.widgets.get("external_rambase_catalog_name")
view_based_rambase_catalog_name = dbutils.widgets.get("view_based_rambase_catalog_name")

print(
    f"External Rambase Catalog Name: {external_rambase_catalog_name}\nView-Based Rambase Catalog Name: {view_based_rambase_catalog_name}"
)

# COMMAND ----------
# MAGIC %md
# MAGIC # Documents Daily
# MAGIC This notebook is responsible for creating and storing daily data extracted from v_procedures

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Create daily table

# COMMAND ----------
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {view_based_rambase_catalog_name}.rambase.documents_daily (
  folder_name STRING, title STRING, status STRING, document_life_cycle_status STRING, id INT,
    document_id CHAR(36), type STRING, version INT, author STRING, verifier STRING, approver STRING,
    created_date STRING, sent_for_verification STRING, verified_date STRING, sent_for_approval
    STRING, approved STRING, next_revision_before STRING, archiver STRING, archived BOOLEAN,
    changelog STRING, feedback STRING, site_id CHAR(36), folder_id INT, substitute_author STRING,
    substitute_approver STRING, substitute_verifier STRING, active_author_id STRING, date DATE, last_updated timestamp 
) USING delta;
"""
spark.sql(create_table_sql)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Merge data from v_procedures into table with current date
# MAGIC Merges data into documents_daily from the source table v_procedures. If the document_id combined with date does not exist in the table then a new row will be inserted.

# COMMAND ----------
merge_sql = f"""
MERGE INTO {view_based_rambase_catalog_name}.rambase.documents_daily AS t
USING (
  SELECT
    folder_name,
    title,
    status,
    document_life_cycle_status,
    id,
    document_id,
    type,
    version,
    author,
    verifier,
    approver,
    created_date,
    sent_for_verification,
    verified_date,
    sent_for_approval,
    approved,
    next_revision_before,
    archiver,
    archived,
    changelog,
    feedback,
    site_id,
    folder_id,
    substitute_author,
    substitute_approver,
    substitute_verifier,
    active_author_id,
    current_date() as `date`,
    current_timestamp() as last_updated
  FROM
    {view_based_rambase_catalog_name}.rambase.v_documents
) AS s
ON t.document_id = s.document_id and t.date = s.date
WHEN NOT MATCHED THEN
INSERT(
  folder_name,
  title,
  status,
  document_life_cycle_status,
  id,
  document_id,
  type,
  version,
  author,
  verifier,
  approver,
  created_date,
  sent_for_verification,
  verified_date,
  sent_for_approval,
  approved,
  next_revision_before,
  archiver,
  archived,
  changelog,
  feedback,
  site_id,
  folder_id,
  substitute_author,
  substitute_approver,
  substitute_verifier,
  active_author_id,
  `date`,
  last_updated
)
VALUES(
  s.folder_name,
  s.title,
  s.status,
  s.document_life_cycle_status,
  s.id,
  s.document_id,
  s.type,
  s.version,
  s.author,
  s.verifier,
  s.approver,
  s.created_date,
  s.sent_for_verification,
  s.verified_date,
  s.sent_for_approval,
  s.approved,
  s.next_revision_before,
  s.archiver,
  s.archived,
  s.changelog,
  s.feedback,
  s.site_id,
  s.folder_id,
  s.substitute_author,
  s.substitute_approver,
  s.substitute_verifier,
  s.active_author_id,
  s.date,
  s.last_updated
)
"""
spark.sql(merge_sql)
