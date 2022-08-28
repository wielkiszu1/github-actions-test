# Databricks notebook source
al_in_file_format = "json"
al_out_format = "delta"
al_table = 'cit_schema_al'
al_schema_location="/mnt/hub/schema"
al_json_loc ="/mnt/hub/to_process" 
al_checkpoint_path = '/mnt/hub/_checkpoints'

# COMMAND ----------

def delete_mounted_dir(dirname):
    files=dbutils.fs.ls(dirname)
    for f in files:
        if f.isDir():
            delete_mounted_dir(f.path)
        dbutils.fs.rm(f.path, recurse=True)

# COMMAND ----------

# dbutils.fs.rm(al_checkpoint_path, recurse = True)
delete_mounted_dir(al_checkpoint_path)

# COMMAND ----------

dbutils.fs.rm(al_checkpoint_path, recurse = True)

# COMMAND ----------

dbutils.fs.rm(al_json_loc,recurse = True)
dbutils.fs.mkdirs(al_json_loc)

# COMMAND ----------

dbutils.fs.cp('/mnt/hub/json_arch/citation_Record.json', '/mnt/hub/to_process/citation_Record.json')

# COMMAND ----------

from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import current_timestamp
stream = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", al_in_file_format)\
    .option("cloudFiles.schemaLocation", al_schema_location)\
    .option("cloudFiles.inferColumnTypes", True) \
    .option("multiLine", True) \
    .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
    .option("overwriteSchema", "true")\
    .load(al_json_loc)\
    .withColumn("CurrTime",current_timestamp())\
    .writeStream.format(al_out_format) \
    .option("mergeSchema", "true")\
    .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
    .option('checkpointLocation', al_checkpoint_path) \
    .trigger(availableNow=True) \
    .table(al_table) 

# COMMAND ----------


