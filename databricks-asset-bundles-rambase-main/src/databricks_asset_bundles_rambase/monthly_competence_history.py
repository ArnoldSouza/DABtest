# Databricks notebook source
"""Databricks notebook for monthly competence history."""

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
# MAGIC # User competence daily
# MAGIC This notebook is responsible for creating and storing daily data extracted from v_competence

# COMMAND ----------
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {view_based_rambase_catalog_name}.rambase.competence_monthly (
  id CHAR(36), role_id CHAR(36), requirement_id CHAR(36), user_id CHAR(36), department_id
    CHAR(36), site_id STRING, document_id CHAR(36), reading_status STRING, cosigner_id CHAR(36),
    cosigning_comment STRING, cosigned_at STRING, expire_at STRING, training_status STRING,
    competence_status STRING, date DATE, last_updated timestamp
) USING delta;
"""
spark.sql(create_table_sql)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Merge data from v_competence into table with current date
# MAGIC Merges data into competence daily from the source table v_competence. If the id combined with date does not exist in the table then a new row will be inserted.

# COMMAND ----------
merge_sql = f"""
MERGE INTO {view_based_rambase_catalog_name}.rambase.competence_monthly AS t
USING (
    SELECT
        id,
        role_id,
        requirement_id,
        user_id,
        department_id,
        site_id,
        document_id,
        reading_status,
        cosigner_id,
        cosigning_comment,
        cosigned_at,
        expire_at,
        training_status,
        competence_status,
        current_date() as `date`,
        current_timestamp() as last_updated
    FROM {external_rambase_catalog_name}.dbo.competence
) as s
ON t.document_id IS NOT DISTINCT FROM  s.document_id and t.user_id = s.user_id and t.role_id = s.role_id and t.requirement_id IS NOT DISTINCT FROM  s.requirement_id and t.date = s.date
WHEN NOT MATCHED THEN
INSERT (
    id,
    role_id,
    requirement_id,
    user_id,
    department_id,
    site_id,
    document_id,
    reading_status,
    cosigner_id,
    cosigning_comment,
    cosigned_at,
    expire_at,
    training_status,
    competence_status,
    `date`,
    last_updated
)
VALUES(
    s.id,
    s.role_id,
    s.requirement_id,
    s.user_id,
    s.department_id,
    s.site_id,
    s.document_id,
    s.reading_status,
    s.cosigner_id,
    s.cosigning_comment,
    s.cosigned_at,
    s.expire_at,
    s.training_status,
    s.competence_status,
    s.date,
    s.last_updated
);
"""
spark.sql(merge_sql)
