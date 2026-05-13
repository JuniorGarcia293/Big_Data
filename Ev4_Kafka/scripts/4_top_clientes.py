# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, desc

spark = SparkSession.builder \
    .appName("Ranking_Clientes_Final") \
    .config("spark.mongodb.input.uri", "mongodb://mongodb:27017/Retail_DB.consolidado_ventas") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/Empresa.top_clientes") \
    .getOrCreate()

# 1. Leer y limpiar
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
df_limpio = df.withColumn("monto", col("monto").cast("double"))

# 2. Agrupar y Ordenar de forma descendente
df_ranking = df_limpio.groupBy("nombre", "nivel_lealtad") \
    .agg(sum("monto").alias("total_gastado")) \
    .orderBy(desc("total_gastado"))

# 3. EL TRUCO PARA MONGO: .coalesce(1)
# Esto obliga a Spark a juntar todo en una sola pieza de datos
# manteniendo el orden de arriba hacia abajo antes de enviarlo a MongoDB.
df_final_para_guardar = df_ranking.coalesce(1)

print("\n" + "="*60)
print("  PROCESANDO RANKING FINAL (MAYOR A MENOR)")
print("="*60)

try:
    # 4. Guardar con sobreescritura total
    df_final_para_guardar.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode("overwrite") \
        .save()
    
    # Mostrar en consola el ranking final (solo para verificar, no es necesario para MongoDB)
    df_ranking.show()
    print("---------------------------------------")
    print(">>> STATUS: OK")
    print(">>> LISTO: Los top clientes ya están guardados en MongoDB.")
    print("---------------------------------------")
except Exception as e:
    print("---------------------------------------")
    print(">>> STATUS: ERROR")
    print("\n Error: ", e)
    print("---------------------------------------")

spark.stop()