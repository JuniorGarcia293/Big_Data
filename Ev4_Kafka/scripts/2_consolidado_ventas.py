# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Consolidado_Ventas_Productos") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/Retail_DB.consolidado_ventas") \
    .getOrCreate()

# 1. Cargar fuentes
df_ventas = spark.read.csv("file:///tmp/data/ventas_*.csv", header=True, inferSchema=True)
df_clientes = spark.read.option("multiLine", "true").json("file:///tmp/data/clientes.json")
df_productos = spark.read.option("multiLine", "true").json("file:///tmp/data/productos.json")

# 2. Joins
ventas_con_clientes = df_ventas.join(df_clientes, "id_cliente")
df_final = ventas_con_clientes.join(df_productos, "id_producto")

# 3. Seleccion de columnas (Sin tildes para evitar error)
df_reporte = df_final.select(
    "id_transaccion",
    "nombre",
    "nombre_producto",
    "categoria",
    "monto",
    "pais",
    "nivel_lealtad"
)

# 4. Guardar en MongoDB
print("\n>>> PROCESANDO Y GUARDANDO EN MONGODB...")

try:
    df_reporte.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").save()
    print("---------------------------------------")
    print(">>> STATUS: OK")
    print(">>> CARGA EXITOSA: Los datos de ventas ya estan en MongoDB.")
    print("---------------------------------------")
except Exception as e:
    print("---------------------------------------")
    print(">>> STATUS: ERROR")
    print(">>> ERROR AL GUARDAR: Verifique la conexion a MongoDB")
    print("---------------------------------------")

spark.stop()