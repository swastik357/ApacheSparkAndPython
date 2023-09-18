import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("/Users/swastik./SparkCourse/book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

'''
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
'''

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