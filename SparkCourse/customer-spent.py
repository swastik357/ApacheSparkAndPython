#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 14 18:06:39 2023

@author: swastik.
"""

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerSpent")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amountSpent = float(fields[2])
    return (customerID,amountSpent)

lines = sc.textFile("/Users/swastik./SparkCourse/customer-orders.csv")
parsedLines = lines.map(parseLine)

##customerIndSpent = parsedLines.map(lambda x: x[1]);
customerTotalSpent = parsedLines.reduceByKey(lambda x, y: x+y)
customerTotalSpentSorted = customerTotalSpent.map(lambda x: (x[1], x[0])).sortByKey().collect()


for result in customerTotalSpentSorted:
    customer = str(result[1]).encode('ascii', 'ignore')
    if customer:
        print(customer.decode()+":\t\t" +"{:.2f}".format(result[0]));
     
    
    
