#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Reference:
# https://www.cloudera.com/tutorials/setting-up-a-spark-development-environment-with-python.html
# HDFS Syntax: hdfs:///tmp/csv-to-json/employee.csv
# Sandbox Deployment:
# Change to project directory: cd C:\JMCube\Make\projects\python\pyspark\pyspark-tutorial
# Copy to HDP sandbox: scp -P 2222 ./csv-to-json-hdp-pyspark.py root@sandbox-hdp.hortonworks.com:/root
# Open sandbox shell: sh -p 2222 root@sandbox-hdp.hortonworks.com
# Use spark-submit to run the program: spark-submit ./csv-to-json-hdp-pyspark.py
# Delete HDFS directory: hdfs dfs -rm -r /tmp/csv-to-json/output

import os
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

def write_json(df, filepath):

    try:
        output = subprocess.Popen(["hdfs", "dfs", "-rm", "-r", filepath], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as inst:
        print(type(inst))  # the exception instance
        print(inst.args)  # arguments stored in .args
        print(inst)  # __str__ allows args to be printed directly

    df.write.json(filepath)

    # for root, dirs, files in os.walk(filepath):
    #     for file in files:
    #         if file.endswith('.json'):
    #             print("\nContents of {}:".format(filepath + "/" + file))
    #             print(open(filepath + "/" + file, "r").read())

spark = SparkSession.builder.appName('csv-to-json').getOrCreate()

# Read CSV and create dataframe
print("Reading CSV ...")
csvdf = spark.read.csv("hdfs:///tmp/csv-to-json/employee.csv", header=True, inferSchema=True)
csvdf.printSchema()
csvdf.show()

# Derive CSV fields
print("Deriving fields ...")
lkpdf = spark.read.csv("hdfs:///tmp/csv-to-json/department.csv", header=True, inferSchema=True)
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
write_json(jsondf, "hdfs:///tmp/csv-to-json/output")
