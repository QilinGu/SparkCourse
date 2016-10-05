from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

# find avg num of friends by age
# sample data 
# 0,Will,33,385
# 1,Jean-Luc,26,2
# 2,Hugh,55,221
# 3,Deanna,40,465

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    # like (33,385)
    return (age, numFriends)

lines = sc.textFile("data/fakefriends.csv")
# get a key/value pair rdd
rdd = lines.map(parseLine)
# use rdd.mapValues if transformation doesn't touch keys, more efficient
# (33,385) => (33,(385,1)), add 1 for counting purpose
# reduceByKey, use lambda to reduce tuples with same age
# below two are pyspark.rdd.PipelinedRDD type
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

results = averagesByAge.collect() # list type
for age, num_friends in results:
    print age,num_friends
