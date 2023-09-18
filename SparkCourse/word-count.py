from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("/Users/swastik./SparkCourse/book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

maxCount = 0
maxCountWord = ''
minCount =100000
minCountWord = ''

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord and (count>maxCount):
        maxCount = count
        maxCountWord  = cleanWord
    elif cleanWord and (count<minCount):
        minCount = count
        minCountWord  = cleanWord    
        
print(maxCountWord.decode() + " " + str(maxCount))
print(minCountWord.decode() + " " + str(minCount))
        
'''
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
'''
