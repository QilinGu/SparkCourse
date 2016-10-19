from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    """
    Data looks like
    ITE00100554,18000101,TMAX,-75,,,E,
    ITE00100554,18000101,TMIN,-148,,,E,
    EZE00100082,18000101,TMIN,-135,,,E,
    ...
    Parse out stationID,entryType and temperature
    """
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("../data/1800.csv")
parsed_lines = lines.map(parseLine) # pick out useful info
min_temps = parsed_lines.filter(lambda x: "TMIN" in x[1]) #pick only TMIN type
station_temps = min_temps.map(lambda x: (x[0],x[2])) #drop entryType filed
min_temps = station_temps.reduceByKey(lambda x,y: min(x,y)) #find min
results = min_temps.collect() #collect data to master

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
    # ITE00100554 5.36F
    # EZE00100082 7.70F

# Think of rdd as three parts of your file
# Think of lines as a list of line, similar API with list and its elements
# Think each line as a string, dictionary, tuple etc.
