from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('helloWorld').getOrCreate()
srcdf = spark.read.csv('training.csv', header=True, inferSchema=True)
srcdf.write.format("json").text()

