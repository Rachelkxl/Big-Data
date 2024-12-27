from pyspark.sql import SparkSession, functions, types
import sys

observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
])


def main (inputs, out):
    try:
        weather = spark.read.csv(inputs, schema=observation_schema)
        weather.createOrReplaceTempView('weather')
        data_filtered = spark.sql('SELECT date, station, observation, value FROM weather WHERE qflag IS NULL AND (observation = "TMAX" OR observation = "TMIN")')   
        data_filtered.createOrReplaceTempView('data_filtered')     
        tmax = spark.sql('SELECT date, station, value / 10 AS temp_max FROM data_filtered WHERE observation = "TMAX"')
        tmin = spark.sql('SELECT date, station, value / 10 AS temp_min FROM data_filtered WHERE observation = "TMIN"')
        tmax.createOrReplaceTempView('tmax')
        tmin.createOrReplaceTempView('tmin')
        range_with_date_and_station = spark.sql('SELECT tmax.date, tmax.station, ROUND(temp_max-temp_min, 1) AS range FROM tmax JOIN tmin ON tmax.date = tmin.date AND tmax.station = tmin.station')
        range_with_date_and_station.createOrReplaceTempView('range_with_date_and_station')
        max_range_each_day = spark.sql('SELECT date, MAX(range) AS max_range FROM range_with_date_and_station GROUP BY date')
        max_range_each_day.createOrReplaceTempView('max_range_each_day')
        max_range_by_station = spark.sql("""
                            SELECT range_with_date_and_station.date, range_with_date_and_station.station, range FROM range_with_date_and_station \
                            JOIN max_range_each_day ON \
                                         range_with_date_and_station.date = max_range_each_day.date AND \
                                         range_with_date_and_station.range = max_range_each_day.max_range
                            """)
        max_range_by_station.createOrReplaceTempView('max_range_by_station')
        max_range_by_station_sorted = spark.sql('SELECT date, station, range FROM max_range_by_station ORDER BY range DESC, date ASC, station ASC')
        max_range_by_station_sorted.write.csv(out, mode='overwrite')
        
    except Exception as e:
        print('Something went wrong: ', e)
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Weather and Temperature Ranges SQL').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
