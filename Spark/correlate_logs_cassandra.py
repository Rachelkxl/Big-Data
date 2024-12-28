# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions correlate_logs_cassandra.py kla298 nasalogs
from pyspark.sql import SparkSession, functions, types
import sys

def main(userid, table_name):
    nasalogs_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table=table_name, keyspace=userid).load()
    # get xi
    count_requests = nasalogs_df.groupby('host').count().withColumnRenamed('count', 'xi')
    # get yi
    sum_request_bytes = nasalogs_df.groupby('host').sum('bytes').withColumnRenamed('sum(bytes)', 'yi')
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
    # calculate the correlation coefficient
    r = (n * sum_xiyi - sum_xi * sum_yi) / (((n * sum_xi2 - sum_xi ** 2) ** 0.5) * ((n * sum_yi2 - sum_yi ** 2)) ** 0.5)
    print("r = ", round(r, 6))
    print("r^2 = ", round(r ** 2, 6))
    #print("Built-in corr = ", new_df.corr('xi','yi', method='pearson'))



if __name__ == "__main__":
    userid = sys.argv[1]
    table_name = sys.argv[2]
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('NASA Logs with Spark and Cassandra') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    main(userid, table_name)


