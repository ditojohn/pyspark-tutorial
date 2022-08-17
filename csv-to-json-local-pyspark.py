#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 12 13:59:32 2022

@author: ditoj
@references:
https://www.youtube.com/watch?v=zhbMKX9eIPM
"""

import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

def write_json(df, filepath):
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
csvdf = spark.read.csv('employee.csv', header=True, inferSchema=True)
csvdf.printSchema()
csvdf.show()

# Derive CSV fields
print("Deriving fields ...")
csvdf = csvdf.withColumn('RevSalary', csvdf.Salary*1.1)

csvdf.printSchema()
csvdf.show()

# Convert to JSON dataframe
print("Converting to JSON ...")
print(csvdf.toJSON().collect())
write_json(csvdf, "JSON")


# Map to Nested JSON schema and Convert
# https://stackoverflow.com/questions/58033947/create-nested-json-out-of-pyspark-dataframe
print("Mapping and converting to Nested JSON ...")
nestdf = (csvdf
          .withColumn('Financials', f.struct(csvdf.Salary, csvdf.RevSalary))
          .select('Name', 'Age', 'Financials'))

print(nestdf.toJSON().collect())
write_json(nestdf, "nestedJSON")

# Lookup values
# https://www.educba.com/pyspark-broadcast-join/
# https://henning.kropponline.de/2016/12/11/broadcast-join-with-spark/
# https://stackoverflow.com/questions/34053302/pyspark-and-broadcast-join-example
# https://sparkbyexamples.com/pyspark/pyspark-broadcast-variables/
print("Looking up values to Nested JSON ...")
lkpdf = spark.read.csv('department.csv', header=True, inferSchema=True)
lkpdf.printSchema()
lkpdf.show()

finaldf = csvdf.join(f.broadcast(lkpdf), csvdf.Dept == lkpdf.Dept).select('Name', 'Age', csvdf.Dept, 'Manager')
write_json(finaldf, "lookupJSON")