#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 16 10:17:04 2023

@author: swastik.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()


movieNames = {}
with codecs.open("/Users/swastik./SparkCourse/ml-100k/u.item", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]




# Create schema when reading u.data
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

# Load up movie data as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("/Users/swastik./SparkCourse/ml-100k/u.data")

# Some SQL-style magic to sort all movies by popularity in one line!
topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

# Grab the top 10
##topMovieIDs.show(10)

results = topMovieIDs.collect()

print("movieId count   movieTitle")
for i in range(10):
    result = results[i]
    print(str(result[0]) + "\t{:.2f}\t".format(result[1])+ movieNames[result[0]])
 
# Stop the session
spark.stop()
