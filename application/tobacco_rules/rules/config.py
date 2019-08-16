#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/5/30 10:18
import os

#-----hbase配置
hbase_host="10.72.59.89"
# hbase_host="10.18.0.34"
hbase_pool_size=10

zkUrl="10.72.59.91:2181"
# zkUrl="10.18.0.34:2181"


phoenix_home="/opt/module/apache-phoenix-4.14.1-HBase-1.2-bin"


#com_id : "011114305", "011114306", "011114302"  邵阳 岳阳 株洲

cities = ["011114305", "011114306", "011114302"]
# cities=["011114313","011114312","011114301","011114305","011114306",
#         "011114307","011114331","011114302","011114309","011114308",
#         "011114310","011114311","011114304","011114303"]

#-----hbase表

table_prefix="V630_TOBACCO."

#零售户画像表
retail_table=table_prefix+"RETAIL"
#区域画像表
area_table=table_prefix+"AREA"
#邵阳预警表
retail_warning_table=table_prefix+"RETAIL_WARNING"
#岳阳 预警表
warning_code_table=table_prefix+"WARNING_CODE"
#岳阳分块统计表
block_data_table=table_prefix+"BLOCK_DATA"
#地区宏观经济指标表
data_index_table=table_prefix+"DATA_INDEX"

#品牌id映射表
brand_table=table_prefix+"BRAND_DATA"
#卷烟动态画像表
ciga_table=table_prefix+"CIGA_PICTURE"
#货源投放表
goods_supply_table=table_prefix+"GEARS_TOSS"
#als 评分表
als_table=table_prefix+"CIGA_GRADE"
#卷烟静态表
cigar_static_table=table_prefix+"CIGA_STATIC"



root_dir="/user/entrobus/tobacco_data_630/"
# root_dir="/user/zhangzy/v_630/"
#----------外部数据路径----------

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

#租金(房天下)、餐饮(大众点评)、酒店数据(大众点评)
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


#卷烟属性
cigar_property_path="/user/entrobus/tobacco_data_630/cigar_property"

#白名单
white_list=os.path.join(root_dir,"white_list/whitelist.csv")



#----------中间数据路径----------

#零售户周边人流数
avg_vfr_path=os.path.join(root_dir,"cust_avg_vfr")

#所有零售户消费水平
cons_level_path=os.path.join(root_dir,"cust_consume_level")

#零售户聚类数据
cluster_path=os.path.join(root_dir,"cluster_result")

#零售户对每品规卷烟评分
cigar_rating_path=os.path.join(root_dir,"cigar_rating")
