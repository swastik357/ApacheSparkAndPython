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

'''
heroNames = spark.read.text("/Users/swastik./SparkCourse/Marvel-Names.txt").collect()
heroDict = {}
for i in range(len(heroNames)):
    line = heroNames[i].value
    x=line[0:line.index(' ')];
    y=line[line.index(' ') + 1:];
    heroDict[int(x)] = y[1:]
'''

Popular = connections.sort(func.col("connections").desc()).collect()
for i in range(20):
    line = Popular[i]
    #print(line[0],line[1])
    first = names.filter(func.col("id") == int(line[0])).select("name").first()[0]
    
    #first,second = heroDict[int(line.id)][:-1],str(line.connections)
    print(first+ " "*(25-len(first))+str(line[1])+"  "+line[0])

#mostPopularName = names.filter(func.col("id") == 1).select("name").first()
#print(mostPopularName[1])

'''    
mostPopular = connections.sort(func.col("connections").desc()).first()


mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

'''