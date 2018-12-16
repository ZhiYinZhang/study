#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/11 9:22
from __future__ import print_function
import time
import threading
import sys
if sys.version >= '3':
    import queue as Queue
else:
    import Queue

from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession

def delayed(seconds):
    def f(x):
        time.sleep(seconds)
        return x
    return f

def call_in_background(f,*args):
    result = Queue.Queue(1)
    t = threading.Thread(target=lambda:result.put(f(*args)))
    t.daemon = True
    t.start()
    return result

def main():
    spark = SparkSession.builder \
                        .appName("status_api") \
                        .master("local[2]") \
                        .config("spark.ui.showConsoleProgress","false") \
                        .getOrCreate()

    sc:SparkContext = spark.sparkContext

    def run():
        rdd = sc.parallelize(range(10),10).map(delayed(2))
        reduced = rdd.map(lambda x:(x,1)).reducebyKey(lambda x,y:x+y)
        return reduced.map(delayed(2)).collect()

    result = call_in_background(run)

    status = sc.statusTracker()

    while result.enpty():
        ids = status.getJobIdsForGroup()
        for id in ids:
            job = status.getJobInfo(id)
            print("Job",id,"status:",job.status)
            for sid in job.stageIds:
                info = status.getStageInfo(sid)
                if info:
                    print(""%())
        time.sleep(1)
    print("Job results are:",result.get())
    sc.stop()

if __name__=="__main__":
    main()