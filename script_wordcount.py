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

text_rdd = sc.textFile("hdfs:///user/hadoop/datos_wordcount/wikipedia.txt")

wordcount_rdd = text_rdd \
    .flatMap(clean_text) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)

start = time.time()

wordcount_rdd.saveAsTextFile("hdfs:///user/hadoop/resultados_wordcount/")

end = time.time()
total_time = end - start

print(f"Time: {total_time:.2f} seconds") 

spark.stop()