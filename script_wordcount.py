# Word Count script with PySpark in local mode

from pyspark.sql import SparkSession
import shutil
import os

if os.path.exists("output"):
    shutil.rmtree("output")

spark = SparkSession.builder \
    .appName("WordCount") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

text_rdd = sc.textFile("/home/usuario/Big Data/wikipedia/wikipedia.txt")

count_rdd = text_rdd \
    .flatMap(lambda line : line.split()) \
    .map(lambda word : (word, 1)) \
    .reduceByKey(lambda a, b : a + b)

count_rdd.saveAsTextFile("output")
spark.stop()