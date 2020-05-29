#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/5/25 18:15
import sys
from application.real_time_recommendation.utils import get_spark
from application.real_time_recommendation.config import delta_path,numFiles,partitionCol

def compact_table(numFiles=numFiles):
    print(f"每个分区压缩成{numFiles}个文件")
    spark.read.format("delta")\
         .load(delta_path)\
         .repartition(numFiles)\
         .write\
         .option("dataChange","false")\
         .format("delta")\
         .partitionBy(partitionCol)\
         .mode("overwrite")\
         .save(delta_path)
    print(f"压缩成功!")
if __name__=="__main__":
    numFiles=int(sys.argv[1])
    spark = get_spark()
    compact_table(numFiles)