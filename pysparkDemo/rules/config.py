#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/30 9:31
import os
#hbase配置
hbase_host="10.72.32.26"
hbase_pool_size=10


#------hbase表
#零售户画像表
retail_table="TOBACCO.RETAIL"
#区域画像表
area_table="TOBACCO.AREA"
#邵阳预警表
retail_warning_table="TOBACCO.RETAIL_WARNING"
#岳阳 预警表
warning_code_table="TOBACCO.WARNING_CODE"
#岳阳分块统计表
block_data_table="TOBACCO.BLOCK_DATA"
#地区宏观经济指标表
data_index_table="TOBACCO.DATA_INDEX"






root_dir="/user/entrobus/tobacco_data/"

#----------外部数据----------


#poi数据
poi_path=os.path.join(root_dir,"poi")


#城市 与 sale_center_id 对应关系
center_path=os.path.join(root_dir,"saleCenterId_with_abcode")

#零售户经纬度
lng_lat_path=os.path.join(root_dir,"lng_lat")

#城市人口数据
population_path=os.path.join(root_dir,"population")

#城市分块数据
block_path=os.path.join(root_dir,"city_block_data")

#租金、餐饮、酒店数据
rent_path=os.path.join(root_dir,"rent_food_hotel/rent.csv")
food_path=os.path.join(root_dir,"rent_food_hotel/food.csv")
hotel_path=os.path.join(root_dir,"rent_food_hotel/hotel.csv")


#城市统计数据 gdp、产业总量等
shaoyang_stat_path=os.path.join(root_dir,"gdp/邵阳统计数据.csv")
yueyang_stat_path=os.path.join(root_dir,"gdp/岳阳统计数据.csv")
zhuzhou_stat_path=os.path.join(root_dir,"gdp/株洲统计数据.csv")


#人流数据
shaoyang_vfr_path=os.path.join(root_dir,"visitor_flow/shaoyang")
yueyang_vfr_path=os.path.join(root_dir,"visitor_flow/yueyang")
zhuzhou_vfr_path=os.path.join(root_dir,"visitor_flow/zhuzhou")





#----------中间数据----------

#零售户周边平均人流数
avg_vfr_path=os.path.join(root_dir,"cust_avg_vfr")

#所有零售户消费水平
cons_level_path=os.path.join(root_dir,"cust_consume_level")

#零售户聚类数据
cluster_path=os.path.join(root_dir,"cluster_result")

