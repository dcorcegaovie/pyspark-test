from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *

import numpy as np
import tarfile
import csv
import json

conf = SparkConf().setAppName("test_app")\
    .setMaster("local[*]")
sc = SparkContext(conf=conf)
sps = SparkSession(sc)

filename = "items_orders.csv.gz"

ordersSchema = StructType(
    [
        StructField("vertical", StringType(), False),
        StructField("item_id", IntegerType(), False),
        StructField("total_ok_usd_amount", FloatType(), False)
    ]
)
df = sps.read.csv(path=filename,schema=ordersSchema , sep=";")
res = df.filter(df.total_ok_usd_amount > 100)

res.write.save(path="orders.json", format="json", mode="overwrite")