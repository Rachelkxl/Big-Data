from pyspark import SparkConf, SparkContext
import sys

def get_graph(input_txt):
    graph = input_txt.map(lambda x: x.split()) \
            .map(lambda x: (int(x[0].replace(':', '')), list(map(int, x[1:]))))
    return graph

def main(input, output, source, dest):
    input_txt = sc.textFile(input+'/links-simple-sorted.txt')
    graph_rdd = get_graph(input_txt)
    # Initialize another key-value RDD of the graph edges, with the format of (node, (source, distance))
    distances = graph_rdd.map(lambda x: (x[0], (None, 0)) if x[0] == source else (x[0], (None, float('inf'))))
    """
    [
    (1, (None, 0)), 
    (2, (None, inf)), 
    (3, (None, inf)), 
    (4, (None, inf)), 
    (5, (None, inf))
    ]
    """
    for i in range(6):
        joined_edges = distances.join(graph_rdd)
        """
        [
        ('4', ((None, inf), ['3'])), 
        ('5', ((None, inf), [])), 
        ('1', ((None, 0), ['3', '5'])), 
        ('2', ((None, inf), ['4'])), 
        ('3', ((None, inf), ['2', '5']))
        ]
        """
        # Get the new edges by adding 1 to the current node's adjacent nodes
        new_edges = joined_edges.flatMap(lambda x: [(y, (x[0], x[1][0][1] + 1)) for y in x[1][1] if x[1][0][1] < float('inf')])
        """
        [
        (3, (1, 1)), 
        (5, (1, 1))
        ]
        """
        # Update the distances and keep the shortest distance
        distances = distances.union(new_edges).reduceByKey(lambda x, y: x if x[1] < y[1] else y).cache()
        # print(distances.take(10))
        distances.saveAsTextFile(output+'/iter-' + str(i))
        # Break the loop if the destination node has been reached
        if distances.lookup(dest)[0][1] < float('inf'):
            break

    # Backtrack to get the shortest path
    final_path = []
    current_node = dest
    while current_node != source:
        final_path.append(current_node)
        current_node = distances.lookup(current_node)[0][0]
    final_path.append(source)
    final_path.reverse()
    sc.parallelize(final_path).saveAsTextFile(output + '/path')





if __name__ == "__main__":
    conf = SparkConf().setAppName('Shortest Path')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0' 
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = int(sys.argv[3])
    dest = int(sys.argv[4])
    main(inputs, output, source, dest)
