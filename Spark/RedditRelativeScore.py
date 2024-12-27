from pyspark import SparkConf, SparkContext
import sys,json


def get_tuples(x):
    return (x['subreddit'],int(x['score']))

def score_once(x):
    yield (x[0], (1, x[1]))

def add(p1, p2):
    return (p1[0] + p2[0], p1[1] + p2[1])

def get_average(p):
    # filter out the subreddits with averages 0 or negative scores
    if p[1][1]/p[1][0] > 0:
        return (p[0], p[1][1]/p[1][0])

def get_relative_score(x):
    return (x[0]/x[1], x[2])


def main (inputs, out):
    try:
        text = sc.textFile(inputs)
        json_object =  text.map(json.loads).cache()
        pairs = json_object.map(get_tuples)
        flatMappped_pairs = pairs.flatMap(score_once)
        reduced_pairs = flatMappped_pairs.reduceByKey(add)
        average = reduced_pairs.map(get_average)
        average_dict = dict(average.collect())
        # broadcast the average_dict to each executor
        bc = sc.broadcast(average_dict)
        comment_data = json_object.map(lambda x: (x['subreddit'], (x['author'], int(x['score']))))
        # access the broadcasted variable 
        broadcasted_rdd = comment_data.map(lambda x: (x[1][1], bc.value[x[0]], x[1][0]))
        output_rdd = broadcasted_rdd.map(get_relative_score)
        output_rdd.sortBy(lambda x: x[0], ascending=False).saveAsTextFile(out)
    
    except:
        print('Something went wrong')
    

if __name__ == '__main__':
    conf = SparkConf().setAppName('Relative Score Broadcast')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0' # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    out = sys.argv[2]
    main(inputs, out)
