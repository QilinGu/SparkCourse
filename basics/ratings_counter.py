from pyspark import SparkConf, SparkContext
import collections

# Basic config 
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# New Spark Context, in pyspark, a sc is created automatically
sc = SparkContext(conf = conf)

# Create a RDD, core concept
lines = sc.textFile("../data/ml-100k/u.data")
# sample data
# 166 346 1   886397596
# 298 474 4   884182806
# 115 265 2   881171488
ratings = lines.map(lambda x: x.split()[2]) # return a RDD transformed from lines
result = ratings.countByValue() # an action,return collections.defaultdict

# result.items return a list [(u'1', 6110), (u'3', 27145)...]
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

# spark-submit rating-counter.py
# sample output
# 1 6110
# 2 11370
# 3 27145
# 4 34174
# 5 21201