#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/5/18 10:45
from datetime import datetime as dt
kafka_bootstrap_servers="10.18.0.12:9092,10.18.0.28:9092,10.18.0.32:9092"
topic="rating"
kafka_checkpoint="/user/zhangzy/real_time_recommendation/kafka_checkpoint"
delta_checkpoint="/user/zhangzy/real_time_recommendation/delta_checkpoint"

combine_data="/user/delta/combine_data"
delta_path="/user/delta/ratings"

start_date=dt(2020,4,28)