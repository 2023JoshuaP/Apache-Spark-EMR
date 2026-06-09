from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

spark = SparkSession.builder \
    .appName("NYC_Taxi_Analysis") \
    .getOrCreate()

df = spark.read.parquet("s3://prueba-emr-josue-unsa/nyc_data_raw/*.parquet")

df = df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime")) \
       .withColumn("pickup_month", F.month("tpep_pickup_datetime"))

print(f"Total de filas cargadas: {df.count():,}")
print(f"Schema:")
df.printSchema()

start = time.time()

print("\nTOTAL DE VIAJES")
total = df.count()
print(f"Total de viajes: {total:,}")

print("\nPROMEDIO DE DISTANCIA")
df.select(
    F.round(F.avg("trip_distance"), 2).alias("promedio_millas"),
    F.round(F.min("trip_distance"), 2).alias("minimo_millas"),
    F.round(F.max("trip_distance"), 2).alias("maximo_millas")
).show()

print("\nHORAS DE MAYOR TRÁFICO")
df.groupBy("pickup_hour") \
  .agg(F.count("*").alias("total_viajes")) \
  .orderBy(F.desc("total_viajes")) \
  .show(10)

# 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided
print("\nMÉTODOS DE PAGO")
df.groupBy("payment_type") \
  .agg(F.count("*").alias("total_viajes")) \
  .withColumn("descripcion", F.when(F.col("payment_type") == 1, "Tarjeta de crédito")
                               .when(F.col("payment_type") == 2, "Efectivo")
                               .when(F.col("payment_type") == 3, "Sin cargo")
                               .when(F.col("payment_type") == 4, "Disputa")
                               .when(F.col("payment_type") == 5, "Desconocido")
                               .otherwise("Anulado")) \
  .orderBy(F.desc("total_viajes")) \
  .show()

print("\nTOP 10 VIAJES MÁS COSTOSOS")
df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "total_amount",
    "payment_type",
    "PULocationID",
    "DOLocationID"
  ) \
  .orderBy(F.desc("total_amount")) \
  .show(10)

print("\nANÁLISIS POR MES (PARTICIONES)")

df.write \
  .partitionBy("pickup_month") \
  .mode("overwrite") \
  .parquet("s3://prueba-emr-josue-unsa/nyc_data_partitioned/")

print("Datos guardados particionados por mes.")

df_partitioned = spark.read.parquet("s3://prueba-emr-josue-unsa/nyc_data_partitioned/")

print("\nViajes y recaudación por mes:")
df_partitioned.groupBy("pickup_month") \
  .agg(
      F.count("*").alias("total_viajes"),
      F.round(F.avg("trip_distance"), 2).alias("distancia_promedio"),
      F.round(F.sum("total_amount"), 2).alias("recaudacion_total"),
      F.round(F.avg("total_amount"), 2).alias("tarifa_promedio")
  ) \
  .orderBy("pickup_month") \
  .show()

print("\nMes con más viajes nocturnos (22:00 - 05:00):")
df_partitioned.filter((F.col("pickup_hour") >= 22) | (F.col("pickup_hour") <= 5)) \
  .groupBy("pickup_month") \
  .agg(F.count("*").alias("viajes_nocturnos")) \
  .orderBy(F.desc("viajes_nocturnos")) \
  .show()

end = time.time()
print(f"\nTime: {end - start:.2f} seconds")

spark.stop()