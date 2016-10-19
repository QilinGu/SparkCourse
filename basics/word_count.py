import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile('\W+',re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile('../data/book.txt')
words = input.flatMap(normalizeWords) # split into words

# count by value
word_counts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
# sort by value, needs to flip key and value
word_counts_sorted = word_counts.map(lambda x: (x[1],x[0])).sortByKey()
results = word_counts_sorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)