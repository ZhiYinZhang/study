#!/usr/bin/env python
# coding: utf-8
from pyspark import StorageLevel
from datetime import datetime as dt,timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col

"""
1.获取当前有效的零售户co_cust
2.获取已转过经纬度的零售户cust_lng_lat
3.对比co_cust和cust_lng_lat，得到未转过经纬度的零售户及其地址，保存成文件
4.将文件拷贝到有网的机器，转换成百度经纬度
5.将转好的经纬度上传到集群hdfs
6.合并co_cust和新的零售户的经纬度
"""
spark=SparkSession.builder.appName("new cust").getOrCreate()

spark.sql("use aistrong")

cities=[
 '011114301','011114302',
 '011114303','011114304','011114305',
 '011114306','011114307','011114308',
 '011114309','011114310','011114311',
 '011114312','011114313','011114331']

last_date=(dt.now()-timedelta(days=1)).strftime("%Y-%m-%d")
# 获取目前有效的零售户  零售户id,零售店地址,com_id
co_cust=spark.sql(f"select * from DB2_DB2INST1_CO_CUST where dt='{last_date}'")\
             .where((col("status")!="04") & (col("com_id").isin(cities)))\
             .select("cust_id","busi_addr","com_id")


#已转过经纬度的零售户
cust_lng_lat=spark.read.csv("/user/entrobus/tobacco_data_630/lng_lat",header=True)\
                  .select("city","com_id","cust_id","busi_addr","lng","lat")


#所有地市的 comid 与city的映射关系
comid_city=spark.read.csv("/user/entrobus/tobacco_data_630/comId_city",header=True)\
                .withColumnRenamed("城市","city")\
                .select("com_id","city")\
                .distinct()


#获取cust_lng_lat中没有的零售户
new_cust=co_cust.join(f.broadcast(cust_lng_lat),["com_id","cust_id"],"outer")\
                .where(col("lng").isNull())\
                .select("cust_id",co_cust["busi_addr"],"com_id")


new_cust.join(comid_city,"com_id")\
        .select("city","com_id","cust_id","busi_addr")\
        .repartition(1).write\
        .csv("/user/entrobus/zhangzy/dataset/cust_addr",header=True,mode="overwrite")



#----------将文件拷贝到有网络的机器 根据地址获取经纬度,然后再上传到hdfs目录----------



#每个地市的经纬度范围
city_lng_lat=spark.read.csv("/user/entrobus/tobacco_data_630/city_lng_lat",header=True)


#新的转好的经纬度
new_lng_lat=spark.read\
 .csv("/user/entrobus/zhangzy/dataset/cust_lng_lat.csv",header=True)\
 .where(col("lng").isNotNull()|col("lat").isNotNull())




#过滤掉不在对应地市范围内的
scope_lng_lat=new_lng_lat.join(city_lng_lat,"city")\
 .where("lng>=min_lng and lng<=max_lng and lat>=min_lat and lat<=max_lat")\
 .drop("min_lng","max_lng","min_lat","max_lat")


#将原来的零售户经纬度和新增的合并
#cust_lng_lat join co_cust 得到cust_lng_lat里面有经纬度并且目前有效的零售户的经纬度
cust_lng_lat.join(co_cust.select("cust_id"),"cust_id")\
            .unionByName(new_lng_lat)\
            .repartition(1)\
            .write.csv("/user/entrobus/zhangzy/dataset/lng_lat"
                        ,header=True,mode="overwrite")


#删除原来的数据
# hdfs dfs -rm -r -f /user/entrobus/tobacco_data_630/lng_lat/*
#将临时目录 拷贝过来
# hdfs dfs -cp /user/entrobus/zhangzy/dataset/lng_lat/* /user/entrobus/tobacco_data_630/lng_lat



