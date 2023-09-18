#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 15 21:28:12 2023

@author: swastik.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("CustomerOrder").getOrCreate()

schema = StructType([ \
                     StructField("customerID", IntegerType(), True), \
                     StructField("orderId", IntegerType(), True), \
                     StructField("amountSpent", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("/Users/swastik./SparkCourse/customer-orders.csv")
df.printSchema()


# Select only stationID and temperature
customerTotalSpent = df.select("customerID", "amountSpent").groupBy("customerID").agg(func.round(func.sum("amountSpent"),2).alias("total_spent"))
customerTotalSpentSorted = customerTotalSpent.sort("total_spent")
customerTotalSpentSorted.show(customerTotalSpentSorted.count())

                                                  
# Collect, format, and print the results
'''
results = customerTotalSpentSorted.collect()
'''
'''
for result in results:
    print(str(result[0]) + "\t{:.2f}".format(result[1]))
 '''   
spark.stop()