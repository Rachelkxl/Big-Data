from pyspark import SparkConf, SparkContext
import sys
import string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('Wikipedia Popular Page Count')
sc = SparkContext(conf=conf)


def get_tuples(line):
	words = line.split()
	return (words[0], words[1], words[2], int(words[3]), words[4])
	
def filter_tuples(i):
	if i[1] == "en" and i[2] != "Main_Page" and not i[2].startswith("Special:"):
		return i

def get_pairs(i):
	return (i[0], i[3])

def get_composite_pairs(i):
	return (i[0], (i[3], i[2]))

def add(x, y):
	return (x + y)

def compare(x, y):
	return x if x[0] > y[0] else y

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])


# Display the page title and its counts with the maximum view count
text = sc.textFile(inputs)
tuples = text.map(get_tuples)
tuples_filtered = tuples.filter(filter_tuples)
timestamp_title_visits_pairs = tuples_filtered.map(get_composite_pairs)
most_popular_page = timestamp_title_visits_pairs.reduceByKey(compare)

# sort_data = most_popular_page.sortBy(lambda x : x[0])
output_formatted = most_popular_page.map(tab_separated)
output_formatted.saveAsTextFile(output)
