#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Reference:
# https://www.cloudera.com/tutorials/setting-up-a-spark-development-environment-with-python.html
# HDFS Syntax: hdfs:///tmp/shakespeare.txt
# Sandbox Deployment:
# Copy to HDP sandbox: scp -P 2222 ./Main.py root@sandbox-hdp.hortonworks.com:/root
# Open sandbox shell: sh -p 2222 root@sandbox-hdp.hortonworks.com
# Use spark-submit to run the program: spark-submit ./Main.py
# Delete HDFS directory: hdfs dfs -rm -r /tmp/shakespeareWordCount

import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

def write_json(df, filepath):
    #shutil.rmtree(filepath)
    df.write.json(filepath)

    for root, dirs, files in os.walk(filepath):
        for file in files:
            if file.endswith('.json'):
                print("\nContents of {}:".format(filepath + "/" + file))
                print(open(filepath + "/" + file, "r").read())

spark = SparkSession.builder.appName('bcbsa-dm-csvtojson').getOrCreate()

# Read CSV and create dataframe
print("Reading CSV ...")
csvdf = spark.read.csv("hdfs:///tmp/employee.csv", header=True, inferSchema=True)
csvdf.printSchema()
csvdf.show()

# Derive CSV fields
print("Deriving fields ...")
lkpdf = spark.read.csv("hdfs:///tmp/department.csv", header=True, inferSchema=True)
jsondf = (csvdf
          .withColumn('RevSalary', csvdf.Salary * 1.1)  # Derive fields
          .withColumn('Financials', f.struct(csvdf.Salary, csvdf.RevSalary))  # Derive nested fields
          .join(f.broadcast(lkpdf), csvdf.Dept == lkpdf.Dept)  # Lookup values
          .select('Name', 'Age', csvdf.Dept, 'Manager', 'Financials')  # Select subset of fields
          )
jsondf.printSchema()
jsondf.show()

# Write to JSON
print("Writing to JSON ...")
write_json(jsondf, "csv-to-json")
