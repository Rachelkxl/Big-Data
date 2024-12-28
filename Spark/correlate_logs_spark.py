from pyspark import SparkConf
from pyspark.sql import SparkSession, functions, types
import sys, re

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
def get_details(line):
    match = re.findall(line_re, line)
    # ignore the lines that don't match this regex
    if match:
        host, datetime, path, bytes_str = match[0]
        return (host, datetime, path, int(bytes_str))
        

http_schema = types.StructType([
    types.StructField('host', types.StringType()),
    types.StructField('datetime', types.StringType()),
    types.StructField('path', types.StringType()),
    types.StructField('bytes', types.IntegerType())
])


def main(input):
    data_rdd = sc.textFile(input)
    # Get the host and the bytes
    details_rdd = data_rdd.map(get_details).filter(lambda x: x is not None)
    details_df = spark.createDataFrame(details_rdd, schema=http_schema)
    # get xi
    count_requests = details_df.groupby('host').count().withColumnRenamed('count', 'xi')
    # get yi
    sum_request_bytes = details_df.groupby('host').sum('bytes').withColumnRenamed('sum(bytes)', 'yi')
    # get xi^2, yi^2, and xiyi
    new_df = count_requests.join(sum_request_bytes, on='host') 
    new_df = new_df.withColumn('xi^2', new_df['xi']**2)
    new_df = new_df.withColumn('yi^2', new_df['yi']**2)
    new_df = new_df.withColumn('xiyi', new_df['xi'] * new_df['yi'])
    # get n (the sum of hosts)
    n = new_df.count()
    # get sum of xi, yi, xi^2, yi^2, xiyi
    sum_df = new_df.agg(functions.sum('xi'), functions.sum('yi'), functions.sum('xi^2'), functions.sum('yi^2'), functions.sum('xiyi')).collect()[0]
    sum_xi = sum_df['sum(xi)']
    sum_yi = sum_df['sum(yi)']
    sum_xi2 = sum_df['sum(xi^2)']
    sum_yi2 = sum_df['sum(yi^2)']
    sum_xiyi = sum_df['sum(xiyi)']
    # print(sum_xi, sum_yi, sum_xi2, sum_yi2, sum_xiyi)
    # calculate the correlation coefficient
    r = (n * sum_xiyi - sum_xi * sum_yi) / (((n * sum_xi2 - sum_xi ** 2) ** 0.5) * ((n * sum_yi2 - sum_yi ** 2)) ** 0.5)
    print("r = ", round(r, 6))
    print("r^2 = ", round(r ** 2, 6))
    # print("Built-in corr = ", new_df.corr('xi','yi', method='pearson'))

if __name__ == "__main__":
    input = sys.argv[1]
    conf = SparkConf().setAppName('NASA web server logs analysis')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # can get the spark context from the spark session
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    main(input)
