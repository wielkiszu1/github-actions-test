# Databricks notebook source
def stop_al_streams():
    for stream in spark.streams.active:
        print(f"Stopping the stream " + stream.id)
        stream.stop()
        try: stream.awaitTermination()
        except: pass 


# COMMAND ----------

al_in_file_format = "json"
al_out_format = "delta"
al_citation_table = 'ccreports_demo_al'

al_citations_json_loc ="/mnt/cck023/silver/citations-validated"
al_citations_checkpoint_path = '/mnt/cck023/silver/citations-AL/_checkpoints'
al_citations_schema_location = "/mnt/cck023/silver/citations-AL/schema_citations"

# COMMAND ----------

from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import current_timestamp
stream = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", al_in_file_format)\
    .option("cloudFiles.schemaLocation", al_citations_schema_location)\
    .option("cloudFiles.inferColumnTypes", True) \
     .option("multiLine", True) \
     .load(al_citations_json_loc)\
     .withColumn("CurrTime",current_timestamp())\
      .withColumn("filePath",input_file_name())\
     .writeStream.format(al_out_format) \
    .option('checkpointLocation', al_citations_checkpoint_path) \
    .trigger(availableNow=True) \
    .table(al_citation_table) 

# COMMAND ----------

import time
time.sleep(60)
stop_al_streams()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ccreports_demo_al

# COMMAND ----------

# log all files processed:
spark.sql('insert into tblLogs select CurrTime, \'AutoLoader Citations\', filePath, \'\' from ccreports_demo_al')
