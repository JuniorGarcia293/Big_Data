from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, regexp_replace

# Configuracion de Spark
spark = SparkSession.builder \
    .appName("Limpieza_Logs") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/Logs.logs_limpios") \
    .getOrCreate()

# Ruta verificada anteriormente
path = "/tmp/data/logs_acceso.txt"

try:
    # 1. Lectura
    df_raw = spark.read.text(path)

    # 2. Procesamiento (Ajustado a image_c74295.png)
    df_limpio = df_raw.select(
        regexp_replace(split(col("value"), " ").getItem(0), r"\[|\]", "").alias("fecha"),
        split(col("value"), " ").getItem(1).alias("nivel"),
        split(col("value"), " ", 3).getItem(2).alias("mensaje")
    )

    # 3. Escritura en MongoDB (Ajustado a image_c74276.png)
    df_limpio.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").save()
    
    # Mensajes ultra-seguros sin caracteres especiales
    print("---------------------------------------")
    print(">>> STATUS: OK")
    print(">>> MSG: Proceso completado exitosamente; Logs visibles en MongoDB.")
    print("---------------------------------------")

except Exception:
    # Si falla, imprimimos un mensaje generico para evitar errores de codec
    print("---------------------------------------")
    print(">>> STATUS: ERROR")
    print(">>> MSG: Verificar logs internos o ruta de archivo")
    print("---------------------------------------")

finally:
    spark.stop()