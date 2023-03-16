# Databricks notebook source
# DBTITLE 1,Enable easy ETL
# MAGIC %sql
# MAGIC spark.readStream.format("cloudFiles") \
# MAGIC   .option("cloudFiles.format", "json") \
# MAGIC   .option("cloudFiles.schemaLocation", "<path_to_schema_location>") \
# MAGIC   .load("<path_to_source_data>") \
# MAGIC   .writeStream \
# MAGIC   .option("mergeSchema", "true") \
# MAGIC   .option("checkpointLocation", "<path_to_checkpoint>") \
# MAGIC   .start("<path_to_target")

# COMMAND ----------

# DBTITLE 1,Prevent data loss in well-structured data
spark.readStream.format("cloudFiles") \
  .schema(expected_schema) \
  .option("cloudFiles.format", "json") \
  # will collect all new fields as well as data type mismatches in _rescued_data
  .option("cloudFiles.schemaEvolutionMode", "rescue") \
  .load("<path_to_source_data>") \
  .writeStream \
  .option("checkpointLocation", "<path_to_checkpoint>") \
  .start("<path_to_target")

# COMMAND ----------

# DBTITLE 1,Enable flexible semi-structured data pipelines
spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  # will ensure that the headers column gets processed as a map
  .option("cloudFiles.schemaHints",
          "headers map<string,string>, statusCode SHORT") \
  .load("/api/requests") \
  .writeStream \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", "<path_to_checkpoint>") \
  .start("<path_to_target")

# COMMAND ----------

# DBTITLE 1,Transform nested JSON data
spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  # The schema location directory keeps track of your data schema over time
  .option("cloudFiles.schemaLocation", "<path_to_checkpoint>") \
  .load("<source_data_with_nested_json>") \
  .selectExpr(
    "*",
    "tags:page.name",    # extracts {"tags":{"page":{"name":...}}}
    "tags:page.id::int", # extracts {"tags":{"page":{"id":...}}} and casts to int
    "tags:eventType"     # extracts {"tags":{"eventType":...}}
  )

# COMMAND ----------

# DBTITLE 1,Infer nested JSON data
spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  # The schema location directory keeps track of your data schema over time
  .option("cloudFiles.schemaLocation", "<path_to_checkpoint>") \
  .option("cloudFiles.inferColumnTypes", "true") \
  .load("<source_data_with_nested_json>")

# COMMAND ----------

# DBTITLE 1,Load CSV files without headers
df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("rescuedDataColumn", "_rescued_data") \ # makes sure that you don't lose data
  .schema(<schema>) \ # provide a schema here for the files
  .load(<path>)

# COMMAND ----------

# DBTITLE 1,Enforce a schema on CSV files with headers
df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("header", "true") \
  .option("rescuedDataColumn", "_rescued_data") \ # makes sure that you don't lose data
  .schema(<schema>) \ # provide a schema here for the files
  .load(<path>)

# COMMAND ----------

# DBTITLE 1,Ingest image or binary data to Delta Lake for ML
spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "binaryFile") \
  .load("<path_to_source_data>") \
  .writeStream \
  .option("checkpointLocation", "<path_to_checkpoint>") \
  .start("<path_to_target")
