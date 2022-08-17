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

    if os.path.exists(filepath) and os.path.isdir(filepath):
        shutil.rmtree(filepath)

    df.write.json(filepath)
    
    for root, dirs, files in os.walk(filepath):
        for file in files:
            if file.endswith('.json'):
                print("\nContents of {}:".format(filepath + "/" + file))
                print(open(filepath + "/" + file, "r").read())
    

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
