from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("/Users/swastik./SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)

maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationMaxTemps = maxTemps.map(lambda x: (x[0], x[2]))
maxTemps = stationMaxTemps.reduceByKey(lambda x, y: max(x,y))

minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationMinTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationMinTemps.reduceByKey(lambda x, y: min(x,y))

minResults = minTemps.collect();
maxResults = maxTemps.collect();

'''
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
'''

for minresult,maxresult in zip(minResults,maxResults):
    print("Min Temperature of "+minresult[0] + " was {:.2f}F".format(minresult[1])+" whereas Max Temperature of "+maxresult[0] + " was {:.2f}F".format(maxresult[1]))
    
