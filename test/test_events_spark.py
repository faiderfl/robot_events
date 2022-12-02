import pytest 
import src.events_spark as es
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType,TimestampType,LongType

def are_dfs_equal(df1, df2): 
    return (df1.schema.names == df2.schema.names)

def test_process_hourly_sorting_aggregate():
  spark = SparkSession.builder.appName('Events').master('local[*]').getOrCreate()

  dates_sequence= es.create_dates_sequence('2022-11-01','2023-03-31',spark)
  orb_site_machines = es.get_orb_site_machines(spark)

  dates_sequence = dates_sequence.crossJoin(orb_site_machines)
  operating_seconds, blocked_seconds, downtime_seconds = es.get_sorting_states(spark)
  sort_attempts_count, sort_attempts_complete_count = es.get_sort_attempts(spark)
  unload_items_count = es.get_unloads(spark)
  hourly_sorting_aggregate = es.process_hourly_sorting_aggregate(dates_sequence,operating_seconds,blocked_seconds,downtime_seconds,sort_attempts_count,sort_attempts_complete_count,unload_items_count,spark)
  hourly_sorting_aggregate.printSchema()

  schema= StructType([
    StructField('orb_site', StringType(), True),
    StructField('machine_name', StringType(), True),
    StructField('date_hour', StringType(), False),
    StructField('start_time', TimestampType(), False),
    StructField('id_hours', LongType(), False),
    StructField('end_time', TimestampType(), True),
    StructField('timezone', StringType(), True),
    StructField('operating_seconds', LongType(), True),
    StructField('downtime_seconds', LongType(), True),
    StructField('blocked_seconds', LongType(), True),
    StructField('total_attempts', LongType(), True),
    StructField('complete_attempts', LongType(), True),
    StructField('units_unloaded', LongType(), True),
    StructField('local_start_time', TimestampType(), True),
    StructField('local_end_time', TimestampType(), True)])

  hsa= spark.read.schema(schema).parquet('data/test/hourly_sorting_aggregate')
  hsa.printSchema()
    
  assert are_dfs_equal(hsa,hourly_sorting_aggregate)       