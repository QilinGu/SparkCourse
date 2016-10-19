from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf = conf)

def parseLine(line):
    """
    Extract customer price pairs
    44,8602,37.19
    35,5368,65.89
    """
    fields = line.split(",")
    return (int(fields[0]), float(fields[2]))

lines = sc.textFile("../data/customer_orders.csv")
customer_price = lines.map(parseLine)
# aggregate by customer, sum it
total_by_customer = customer_price.reduceByKey(lambda x,y: x+y)
# sort by price, flip it and sort by key
total_sorted = total_by_customer.map(lambda x:(x[1],x[0])).sortByKey()
results = total_sorted.collect()

for r in results:
    print(str(r[1]) + "\t" + str(r[0]))
    # 39 6193.11
    # 73  6206.2
    # 68  6375.45
