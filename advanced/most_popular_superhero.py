from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)

def count_occurence(line):
    elements = line.split()
    return (int(elements[0]),len(elements)-1)

def parse_name(line):
    fields = line.split("\"")
    return (int(fields[0]),fields[1].encode("utf-8"))

names = sc.textFile("../data/marvel_names.txt")
names_rdd = names.map(parse_name) # use rdd, not broadcast this time

lines = sc.textFile("../data/marvel_graph.txt")
pairings = lines.map(count_occurence)
total_friends = pairings.reduceByKey(lambda x,y: x+y)
flipped = total_friends.map(lambda x: (x[1],x[0]))

most_popular = flipped.max() # return a key/value pair,(count,id), not name
most_popular_name = names_rdd.lookup(most_popular[1]) # ['CAPTAIN AMERICA']
most_popular_name = most_popular_name[0]


print(str(most_popular_name) + " is the most popular superhero, with " + \
    str(most_popular[0]) + " co-appearances")
# CAPTAIN AMERICA is the most popular superhero, with 1933 co-appearances
