#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 16 13:00:58 2023

@author: swastik.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("/Users/swastik./SparkCourse/Marvel-Names.txt")

lines = spark.read.text("/Users/swastik./SparkCourse/Marvel-Graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

  
obscureHeros = connections.filter(func.col("connections")==1)

obscureHerosJoin = obscureHeros.join(names,"id")
obscureHerosJoin[['name']].show()

leastPopular = connections.agg(func.min("connections")).first()[0]
print(leastPopular)

leastPopularHeros = connections.filter(func.col("connections")==leastPopular)
leastPopularHerosJoin = leastPopularHeros.join(names,"id")
leastPopularHerosJoin.select("name").show()
##obscureHerosMin = connections.filter((agg(func.min('connections'))))
##mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

##print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")
