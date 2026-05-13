# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, regexp_replace

spark = SparkSession.builder \
    .appName("AA4_Streaming_Logs") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/Certus.logs_procesados") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 1. LEER DE KAFKA
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "topic_logs") \
    .load()

# 2. TRANSFORMACION
df_string = df_kafka.selectExpr("CAST(value AS STRING)")

df_limpio = df_string.select(
    regexp_replace(split(col("value"), " ").getItem(0), r"\[|\]", "").alias("fecha"),
    split(col("value"), " ").getItem(1).alias("nivel"),
    split(col("value"), " ", 3).getItem(2).alias("mensaje")
)

# 3. FUNCION PARA ESCRIBIR EN MONGO (foreachBatch)
def guardar_en_mongo(df, batch_id):
    df.write.format("com.mongodb.spark.sql.DefaultSource") \
      .mode("append") \
      .save()

# 4. INICIAR STREAMING
query = df_limpio.writeStream \
    .foreachBatch(guardar_en_mongo) \
    .option("checkpointLocation", "/tmp/checkpoints_logs") \
    .start()

print("--- ESCUCHANDO KAFKA Y GUARDANDO EN MONGODB ---")
query.awaitTermination()