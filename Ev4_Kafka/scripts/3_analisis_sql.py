# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, count, collect_list, struct, lit

spark = SparkSession.builder \
    .appName("Ranking_Productos_Mas_Vendidos") \
    .config("spark.mongodb.input.uri", "mongodb://mongodb:27017/Retail_DB.consolidado_ventas") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/Empresa.reporte_ventas") \
    .getOrCreate()

# 1. Cargar datos
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

# 2. Agrupar por producto y contar las ventas
df_conteo = df.groupBy("nombre_producto") \
    .agg(count("id_transaccion").alias("cantidad_ventas")) \
    .orderBy(desc("cantidad_ventas"))

# 3. Convertir a Objeto Unico para MongoDB
df_final = df_conteo.select(
    struct("nombre_producto", "cantidad_ventas").alias("detalle")
).agg(
    collect_list("detalle").alias("lista_productos_vendidos")
).withColumn("tipo_reporte", lit("Resumen de Cantidades Vendidas"))

# 4. Guardar en MongoDB
try:
    df_final.coalesce(1).write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode("overwrite") \
        .save()
    
    print("\n" + "="*60)
    print("---------------------------------------")
    print(">>> STATUS: OK")
    print(">>> EXITO: Reporte en cantidades de ventas generado.")
    print("---------------------------------------")
    df_conteo.show() 
    print("="*60 + "\n")
except Exception as e:
    print("---------------------------------------")
    print(">>> STATUS: ERROR")
    print("\n[!] Error: " + str(e))
    print("---------------------------------------")

spark.stop()