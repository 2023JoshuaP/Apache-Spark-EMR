from pyspark.sql import SparkSession
import re
import time

def extract_words(document):
    path = document[0]
    content = document[1]
    file_name = path.split("/")[-1]
    minus_content = content.lower()
    clean_content = re.sub(r'[^\w\s]', '', minus_content)
    words = clean_content.split()
    return list(set([(word, file_name) for word in words]))

spark = SparkSession.builder \
        .appName("InvertedIndex") \
        .getOrCreate()
sc = spark.sparkContext

docs_rdd = sc.wholeTextFiles("s3://prueba-emr-josue-unsa/input_indice/*.txt", 90)

invertedindex_rdd = docs_rdd \
        .flatMap(extract_words) \
        .map(lambda x: (x[0], [x[1]])) \
        .reduceByKey(lambda a, b: a + b)

start_time = time.time()
invertedindex_rdd.saveAsTextFile("s3://prueba-emr-josue-unsa/resultados_indice_invertido_2/")
end_time = time.time()

print(f"Time: {end_time - start_time:.2f} seconds")
spark.stop()