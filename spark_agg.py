import json

import datetime

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("alex_test").getOrCreate()

targ = datetime.datetime(2021,6,1)

df = spark.read.json("*.json.bz2")

import cool

# F.to_date(
#    .filter(F.col('common.td').cast(DateType())  > F.lit(targ))\
#    .filter(F.to_date('common.td') > F.lit(targ))\        

#    .coalesce(1).write.format('json').mode('overwrite').save(path3)
#     .write.format('json').mode('overwrite').save("ggg")
df\
    .filter(F.to_date('common.td') > '2021-06-01')\
    .groupBy('data.port')\
    .agg(\
         F.count('data.risk.eff').alias("cnt"),\
         F.sum('data.risk.eff').alias('foo'))\
    .withColumn('dad', cool.awesome(F.col('foo') , 77))\
    .show()


#.show()


