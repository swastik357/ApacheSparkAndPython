#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 15 19:07:54 2023

@author: swastik.
"""

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("/Users/swastik./SparkCourse/fakefriends-header.csv")
    
people.select("age","friends").groupBy("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort(func.desc("age")).show()

spark.stop()