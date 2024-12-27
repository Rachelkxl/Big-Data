from pyspark.sql import SparkSession, functions, types
import sys

observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    # types.StructField('sflag', types.StringType()),
    # types.StructField('obstime', types.StringType()),
])


def main (inputs, out):
    try:
        weather = spark.read.csv(inputs, schema=observation_schema)
        data_filtered = weather.filter(weather['qflag'].isNull())\
                        .filter((weather['observation'] == 'TMAX') | \
                        (weather['observation'] == 'TMIN')).cache()
        
        TMIN = data_filtered.where(data_filtered['observation']=='TMIN').withColumn('temp_min', data_filtered['value'] / 10)
        TMAX = data_filtered.where(data_filtered['observation']=='TMAX').withColumn('temp_max', data_filtered['value'] / 10)
        max_and_min_with_date_and_station = TMIN.join(TMAX, on=['station', 'date'], how='inner').select('station', 'date', 'temp_max', 'temp_min')
        range_with_date_and_station = max_and_min_with_date_and_station.withColumn('range', functions.round((max_and_min_with_date_and_station['temp_max'] - max_and_min_with_date_and_station['temp_min']), 1)).cache()
       
        max_range_each_day = range_with_date_and_station.groupBy('date').max('range')
        max_range_by_station = range_with_date_and_station.join(max_range_each_day, 
                            (range_with_date_and_station['date'] == max_range_each_day['date']) &
                            (range_with_date_and_station['range'] == max_range_each_day['max(range)']),
                            how='inner').drop(max_range_each_day['date'], 'max(range)')
        
        max_range_by_station = max_range_by_station.select('date', 'station', 'range').sort('range', 'date', 'station', ascending=[0, 1, 1])
        max_range_by_station.write.csv(out, mode='overwrite')
  
        
    except Exception as e:
        print('Something went wrong: ', e)
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Weather and Temperature Ranges DataFrame').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
