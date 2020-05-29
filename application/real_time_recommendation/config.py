#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/5/18 10:45
from datetime import datetime as dt
kafka_bootstrap_servers="10.18.0.12:9092,10.18.0.28:9092,10.18.0.32:9092"
topic="rating"
#spark写kafka时的checkpoint目录
kafka_checkpoint="/user/zhangzy/real_time_recommendation/kafka_checkpoint"
#spark写delta lake时的chekcpoint目录
delta_checkpoint="/user/zhangzy/real_time_recommendation/delta_checkpoint"

#需要流式写入kafka的数据
combine_data="/user/delta/combine_data"
#delta lake目录
delta_path="/user/delta/ratings"

#数据开始日期
start_date=dt(2020,4,28)


numFiles=100
partitionCol="dt"