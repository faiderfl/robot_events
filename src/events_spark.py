from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark import sql
import re
import pyspark.sql.functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructField, StructType, IntegerType, StringType,DoubleType,DecimalType
from decimal import Decimal
from pyspark.sql.functions import substring, col, transform, concat,sequence, explode,monotonically_increasing_id
import pandas as pd
import datetime

spark = SparkSession.builder.appName('Events').master('local[*]').getOrCreate()
sc= spark.sparkContext

dates_sequence = spark.sql("SELECT sequence(to_timestamp('2022-11-01'), to_timestamp('2023-03-31'), interval 1 Hour) as start_time").withColumn("start_time", explode(col("start_time")))
dates_sequence= dates_sequence.withColumn("id_hours", monotonically_increasing_id())
dates_sequence= dates_sequence.withColumn("date_hour", F.date_format('start_time',"yyyy-MM-dd HH"))
dates_sequence= dates_sequence.withColumn("end_time", F.unix_timestamp("start_time") + 3599)
dates_sequence= dates_sequence.withColumn("end_time", F.to_timestamp('end_time'))


orb_site_info = spark.read.options(header='True').options(sep=',').options(infer_schema=True).csv("data/orb_site_info.csv")
orb_site_info = orb_site_info.withColumnRenamed('orb_site','meta_orb_site')
machines= spark.read.options(header='True').options(sep=',').options(infer_schema=True).csv("data/orb_site_machines.csv")

orb_site_machines = orb_site_info.join(machines, ["meta_orb_site"])
dates_sequence = dates_sequence.crossJoin(orb_site_machines)

#------------------------
sorting_states = spark.read.options(header='True').options(sep=',').options(infer_schema=True).csv("data/sorting_states.csv")

sorting_states = sorting_states.withColumn('start_time_datetime', F.to_timestamp(sorting_states['start_time']*1))
sorting_states = sorting_states.withColumn('end_time_datetime', F.to_timestamp(sorting_states['end_time']*1))


unix_checkin = F.unix_timestamp(sorting_states.start_time_datetime)
unix_checkout = F.unix_timestamp(sorting_states.end_time_datetime)

start_hour_checkin = F.date_trunc("hour", sorting_states.start_time_datetime)
unix_start_hour_checkin = F.unix_timestamp(start_hour_checkin)
checkout_next_hour = F.date_trunc("hour", sorting_states.end_time_datetime) + F.expr("INTERVAL 1 HOUR")

diff_hours = F.floor((unix_checkout - unix_start_hour_checkin) / 3600)
next_hour = F.explode(F.transform(F.sequence(F.lit(0), diff_hours), lambda x: F.to_timestamp(F.unix_timestamp(start_hour_checkin) + (x + 1) * 3600)))

minute = (F.when(start_hour_checkin == F.date_trunc("hour", sorting_states.end_time_datetime), (unix_checkout - unix_checkin) / 60)
           .when(checkout_next_hour == F.col("next_hour"), (unix_checkout - F.unix_timestamp(F.date_trunc("hour", sorting_states.end_time_datetime))) / 60)
           .otherwise(F.least((F.unix_timestamp(F.col("next_hour")) - unix_checkin) / 60, F.lit(60)))
         ).cast("int")

second = (F.when(start_hour_checkin == F.date_trunc("hour", sorting_states.end_time_datetime), (unix_checkout - unix_checkin))
           .when(checkout_next_hour == F.col("next_hour"), (unix_checkout - F.unix_timestamp(F.date_trunc("hour", sorting_states.end_time_datetime))))
           .otherwise(F.least((F.unix_timestamp(F.col("next_hour")) - unix_checkin), F.lit(3600)))
         ).cast("int")

sorting_states_seconds = (sorting_states.withColumn("next_hour", next_hour)
    .withColumn("seconds", second)
    .withColumn("minutes", minute)
    .withColumn("hr", F.date_format(F.expr("next_hour - INTERVAL 1 HOUR"), "H"))
    .withColumn("day", F.to_date(F.expr("next_hour - INTERVAL 1 HOUR")))
    .select("id","meta_orb_site","meta_machine_name","start_time_datetime", "end_time_datetime", "next_hour","state", "day", "hr", "minutes","seconds")
).filter("seconds!=0")


sorting_states_seconds= sorting_states_seconds.withColumn("date_hour", F.date_format(concat(sorting_states_seconds.day,F.lit(" "),sorting_states_seconds.hr), "yyyy-MM-dd HH"))



operating_seconds = sorting_states_seconds.filter("state == 'sorting'").groupBy("meta_orb_site","meta_machine_name","date_hour").agg(F.sum(sorting_states_seconds.seconds).alias("operating_seconds"))
downtime_seconds = sorting_states_seconds.filter(("state == 'scheduled_downtime' or state == 'unscheduled_downtime'")).groupBy("meta_orb_site","meta_machine_name","date_hour").agg(F.sum(sorting_states_seconds.seconds).alias("downtime_seconds"))
blocked_seconds = sorting_states_seconds.filter("state == 'blocked'").groupBy("meta_orb_site","meta_machine_name","date_hour").agg(F.sum(sorting_states_seconds.seconds).alias("blocked_seconds"))

hourly_sorting_aggregate = dates_sequence.join(operating_seconds,['meta_orb_site','meta_machine_name','date_hour'],"left").join(blocked_seconds, ['meta_orb_site','meta_machine_name','date_hour'],"left").join(downtime_seconds,['meta_orb_site','meta_machine_name','date_hour'],"left")
#------------------------------------------------------

sort_attempts=spark.read.options(header='True').options(sep=',').options(infer_schema=True).csv("data/sort_attempts.csv")
sort_attempts = sort_attempts.withColumn('start_time_datetime', F.to_timestamp(sort_attempts['start_time']*1))
sort_attempts = sort_attempts.withColumn('end_time_datetime', F.to_timestamp(sort_attempts['end_time']*1))


sort_attempts = sort_attempts.withColumn("date_hour", F.date_format('start_time_datetime',"yyyy-MM-dd HH"))


sort_attempts_count= sort_attempts.groupBy("meta_orb_site","meta_machine_name","date_hour").agg(F.count(sort_attempts.id).alias("total_attempts"))


sort_attempts_complete_count= sort_attempts.filter("outcome == 'complete'").groupBy("meta_orb_site","meta_machine_name","date_hour").agg(F.count(sort_attempts.id).alias("complete_attempts"))
hourly_sorting_aggregate= hourly_sorting_aggregate.join(sort_attempts_count, ['meta_orb_site','meta_machine_name','date_hour'],"left").join(sort_attempts_complete_count, ['meta_orb_site','meta_machine_name','date_hour'],"left")

#------------------------------------------------------------------

unloads= spark.read.options(header='True').options(sep=',').options(infer_schema=True).csv("data/unloads.csv")
unloads_items= spark.read.options(header='True').options(sep=',').options(infer_schema=True).csv("data/unload_items.csv")

unloads = unloads.withColumn('event_time_datetime', F.to_timestamp(unloads['event_time']*1))

unloads_join_items = unloads.join(unloads_items, ["meta_orb_site","meta_machine_name",'unload_id'],"left")
unloads_join_items= unloads_join_items.withColumn("date_hour", F.date_format('event_time_datetime',"yyyy-MM-dd HH"))

unload_items_count = unloads_join_items.groupBy("meta_orb_site","meta_machine_name","date_hour").agg(F.count("item_id").alias("units_unloaded"))

hourly_sorting_aggregate = hourly_sorting_aggregate.join(unload_items_count, ['meta_orb_site','meta_machine_name','date_hour'],"left")

#-----------------------

hourly_sorting_aggregate = hourly_sorting_aggregate.withColumnRenamed("meta_orb_site","orb_site").withColumnRenamed("meta_machine_name","machine_name")
hourly_sorting_aggregate = hourly_sorting_aggregate.withColumn("local_start_time", F.from_utc_timestamp("start_time",hourly_sorting_aggregate.timezone))
hourly_sorting_aggregate = hourly_sorting_aggregate.withColumn("local_end_time", F.from_utc_timestamp("end_time",hourly_sorting_aggregate.timezone))

hourly_sorting_aggregate.sort("meta_orb_site","meta_machine_name","date_hour").na.fill(value=0).filter("(date_hour>= '2022-11-20 19') and (meta_orb_site =='Las Vegas')")\
  .select('orb_site', "machine_name", "start_time","end_time","local_start_time","local_end_time","units_unloaded","total_attempts","complete_attempts","operating_seconds","downtime_seconds","blocked_seconds","timezone")\
  .show(500)
