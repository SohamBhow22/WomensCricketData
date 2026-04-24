from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()
print(spark.range(5).show())