from pyspark import SparkConf, SparkContext

# broadcast u.item to nodes, share movie name, not just id
def load_movie_names():
    # 1|Toy Story (1995)|01-Jan-1995|...
    # 2|GoldenEye (1995)|01-Jan-1995|...
    movie_names = {}
    with open("../data/ml-100k/u.item") as fo:
        for line in fo:
            fields = line.split("|")
            movie_names[int(fields[0])] = fields[1]
    return movie_names

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

# broadcast down to nodes, then name_dict.value return dict movie_names 
name_dict = sc.broadcast(load_movie_names()) # retrive with .value

# count it
lines = sc.textFile("../data/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]),1))
movie_counts = movies.reduceByKey(lambda x,y: x+y)

# sort it
flipped = movie_counts.map(lambda xy: (xy[1],xy[0]))
sorted_movies = flipped.sortByKey()

# broadcast movie names
movies_with_name = sorted_movies.map(lambda (count,movie):(name_dict.value[movie],count))
results = movies_with_name.collect()

for r in results:
    print r
    # ('Contact (1997)', 509)
    # ('Star Wars (1977)', 583)


