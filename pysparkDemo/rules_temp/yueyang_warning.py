#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/8 14:37
import traceback as tb
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pysparkDemo.rules_temp.write_hbase import write_hbase1
from pysparkDemo.rules_temp.utils import *
from pyspark.sql import Window
from datetime import datetime as dt


spark = SparkSession.builder.enableHiveSupport().appName("retail warning").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("WARN")

spark.sql("use aistrong")

hbase={"table":"TOBACCO.RETAIL_WARNING","families":["0"],"row":"cust_id"}
hbase={"table":"test_ma","families":["info"],"row":"cust_id"}

