#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: ditoj
@references:
https://www.youtube.com/watch?v=zhbMKX9eIPM
"""

import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

def write_json(df, filepath):

    try:
        shutil.rmtree(filepath)
    except Exception as inst:
        print(type(inst))  # the exception instance
        print(inst.args)  # arguments stored in .args
        print(inst)  # __str__ allows args to be printed directly

    df.write.json(filepath)
    
    for root, dirs, files in os.walk(filepath):
        for file in files:
            if file.endswith('.json'):
                print("\nContents of {}:".format(filepath + "/" + file))
                print(open(filepath + "/" + file, "r").read())

# Hadoop Settings
os.environ['HADOOP_HOME'] = "C:\JMCube\Make\hadoop"
os.environ['hadoop.home.dir'] = "C:\JMCube\Make\hadoop"

# PySpark Settings
os.environ['PYSPARK_PYTHON'] = "python"
os.environ['PYSPARK_DRIVER_PYTHON'] = "python"
os.environ['SPARK_HOME'] = "C:\JMCube\Make\spark\spark-3.3.0-bin-hadoop2"

# print(os.environ['JAVA_HOME'])
# print(os.environ['HADOOP_HOME'])
# print(os.environ['hadoop.home.dir'])
# print(os.environ['SPARK_HOME'])
# print(os.environ['PYSPARK_PYTHON'])
# print(os.environ['PYSPARK_DRIVER_PYTHON'])
# print(os.environ['PATH'])

spark = SparkSession.builder.appName('csv-to-json').getOrCreate()

# Read CSV and create dataframe
print("Reading CSV ...")
csvdf = spark.read.csv("employee.csv", header=True, inferSchema=True)
csvdf.printSchema()
csvdf.show()

# Derive CSV fields
# https://stackoverflow.com/questions/58033947/create-nested-json-out-of-pyspark-dataframe
# https://www.educba.com/pyspark-broadcast-join/
# https://henning.kropponline.de/2016/12/11/broadcast-join-with-spark/
# https://stackoverflow.com/questions/34053302/pyspark-and-broadcast-join-example
# https://sparkbyexamples.com/pyspark/pyspark-broadcast-variables/

print("Deriving fields ...")
lkpdf = spark.read.csv("department.csv", header=True, inferSchema=True)
jsondf = (csvdf
          .withColumn('RevSalary', csvdf.Salary * 1.1)  # Derive fields
          .withColumn('Financials', f.struct('Salary', 'RevSalary'))  # Derive nested fields
          .join(f.broadcast(lkpdf), csvdf.Dept == lkpdf.Dept)  # Lookup values
          .select('Name', 'Age', csvdf.Dept, 'Manager', 'Financials')  # Select subset of fields
          )
jsondf.printSchema()
jsondf.show()

# Write to JSON
print("Writing to JSON ...")
write_json(jsondf, "csv-to-json")
