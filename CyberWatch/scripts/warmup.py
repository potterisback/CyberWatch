from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("connector-cache").getOrCreate()
spark.range(1).count()
spark.stop()
