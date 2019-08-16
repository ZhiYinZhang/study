#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/7/4 14:30
from datetime import datetime as dt
import time
from rules.generate_data import generate_brand_data,generate_cigar_static_data,generate_cigar_rating

# generate_brand_data()
# generate_cigar_static_data()
generate_cigar_rating()


from rules.write_hbase import create_ph_table,switch_table,rebuild_index
from rules.config import ciga_table,goods_supply_table,als_table
from rules.cigar import get_brand_stats_info_daily,get_brand_stats_info_monthly,get_brand_stats_info_weekly,get_item_ratio,\
                get_item_stats_info_daily,get_item_stats_info_monthly,get_item_stats_info_weekly,get_item_historical_sales,\
                get_item_sales_forecast,get_cover_rate,get_static_similar_ciagr,get_order_similar_cigar,\
                get_brand_rating,get_item_rating

#创建卷烟画像表的临时表
create_ph_table(ciga_table)

get_brand_stats_info_daily()
get_brand_stats_info_monthly()
get_brand_stats_info_weekly()
get_item_ratio()

get_item_stats_info_daily()
get_item_stats_info_monthly()
get_item_stats_info_weekly()
get_item_historical_sales()
get_item_sales_forecast()
get_cover_rate()
get_static_similar_ciagr()
get_order_similar_cigar()

#将临时表重命名为目标表
switch_table(ciga_table)





#创建卷烟评分表的临时表
create_ph_table(als_table)

get_brand_rating()
get_item_rating()

#将临时表重命名为目标表
switch_table(als_table)





from rules.goods_supply import city_reality_supply,county_reality_supply,supply_analysis_city,supply_analysis_county,\
                              source_supply,recommend_supply_value
#创建临时表
create_ph_table(goods_supply_table)

city_reality_supply()
county_reality_supply()
supply_analysis_city()
supply_analysis_county()
source_supply()
recommend_supply_value()

#重命名
switch_table(goods_supply_table)

time.sleep(10)
#重建索引
rebuild_index(goods_supply_table,"v630_tobaccogearstoss")

print(f"{str(dt.now())} end")