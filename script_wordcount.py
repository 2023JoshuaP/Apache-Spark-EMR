from pyspark.sql import SparkSession
import re
import time

def clean_text(line):
    minus_line = line.lower()
    clean_line = re.sub(r'[^\w\s]', '', minus_line)
    return clean_line.split()

spark = SparkSession.builder \
    .appName("WordCount_HDFS_Produccion") \
    .getOrCreate()

sc = spark.sparkContext

# ¡EL CAMBIO CLAVE! Leemos desde HDFS
text_rdd = sc.textFile("hdfs:///user/hadoop/datos_wordcount/wikipedia.txt")

wordcount_rdd = text_rdd \
    .flatMap(clean_text) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)

inicio = time.time()

# Guardamos los resultados de vuelta en HDFS
wordcount_rdd.saveAsTextFile("hdfs:///user/hadoop/resultados_wordcount/")

fin = time.time()
tiempo_total = fin - inicio

print("=====================================================")
print(f"¡PROCESAMIENTO HDFS TERMINADO!")
print(f"Time: {tiempo_total:.2f} seconds") 
print("=====================================================")

spark.stop()