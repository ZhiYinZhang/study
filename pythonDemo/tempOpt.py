#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 14:59
from sqlalchemy import create_engine
import pandas as pd
import pymysql
from datetime import datetime as dt
# user="root"
# passwd="ouhao#18"
# host="120.78.127.137"
# port='3306'
# db_name="ouhaodw"

user="root"
passwd="123456"
host="localhost"
port='3306'
db_name="entrobus"
engine=create_engine(f"mysql+pymysql://{user}:{passwd}@{host}:{port}/{db_name}")

tables={
        "昊居签约数据":"hj_subscription_data",
        "昊居任务额":"hj_task_amount",
        "昊居回款额":"hj_collection_amount",
        "昊居应收账款":"hj_accounts_receivable",
        "昊居类型收入利润":"hj_income_profit",
        "昊居项目净利润":"hj_project_profit",
        "昊居区域利润":"hj_region_profit",
        "昊居利率偏差":"hj_profit_deviation"
       }
cols={
"昊居签约数据":{'年份':'year','归属月份':'month','合同编码':'contract_id','合同名称':'contract_name','合同内容摘要':'contract_abstract',
                        '合同类型':'contract_type','对方单位/客户名称':'second_party','我方签订合同公司':'first_party','合同金额':'contract_amount',
                        '合同减免/折扣':'contract_discount','其他增减项':'other_discount','实际合同金额':'reality_amount','合同签订日期':'contract_date',
                        '签单业务员':'salesman','收款方式':'payment','备注':'remark','检查日期':'checked_date','检查修订后金额':'checked_amount','差额':'difference',
                        '差额原因':'difference_reason','检查情况':'checked_situation','区域':'region','项目名称':'project','项目编码':'project_id','更新时间':'update_time'},
"昊居任务额":{'年份':'year','月份':'month','类型':'type','金额':'amount','更新时间':'update_time'},
"昊居回款额":{'年份':'year','归属月份':'month','回款类型':'collection_type','对方单位/客户名称':'customer','交易时间':'deal_date','交易金额':'deal_amount',
                        '增减项':'fluctuate','实际交易金额':'reality_amount','款项用途':'fund','付款方式':'pay_way','票据编码':'bill_id','是否退款':'refund',
                        '退款原因':'refund_reason','关联合同编码':'relation_contract_id','合同签订日期':'contract_date','签单业务员':'salesman','备注':'remark',
                        '是否未达账项':'outstanding_account','银行存款到账/付出时间':'arrival_time','实际到账金额':'arrival_amount','差额':'difference',
                        '银行账户名称':'bank','银行账号':'bank_account','回款与实际到账时间差额':'time_difference','是否异常':'abnormal','区域':'region',
                        '关联项目编码':'project_id','关联项目名称':'project','更新时间':'update_time'},
"昊居应收账款":{'更新年份':'year','更新月份':'month','客商名称':'customer','经济内容':'economic_content','期末账面余额':'account_balance',
                          '<1个月':'less_month','1个月~3个月':'between_1_and_3_month','3个月~6个月':'between_3_and_6_month','6个月~1年':'between_6_and_12_month',
                          '1-2年':'between_1_and_2_year','2-3年':'between_2_and_3_year','3年以上':'top_3year','总计':'total_amount','更新时间':'update_time'},
"昊居类型收入利润":{'年份':'year','月份':'month','毛利率':'rate_margin','净利率':'net_margin','类型':'type','更新时间':'update_time'},
"昊居项目净利润":{'年份':'year','月份':'month','项目名称':'project','净利润':'net_profit','更新时间':'update_time'},
"昊居区域利润":{'区域':'region','年份':'year','月份':'month','净利润':'net_profit','净利率':'net_margin','更新时间':'update_time'},
"昊居利率偏差":{'年份':'year','月份':'month','类型':'type','累计利率':'accumulation_rate','目标利率':'target_rate','偏差':'deviation','更新时间':'update_time'}
}

path="e://test//昊居数据库字段(6).xlsx"
# path="e://test//千摩销售目标及回款2019(1).xlsx"


def excel_to_mysql(file,tables,cols):
    #excel 表
    excel_tables=list(tables.keys())

    update_time = str(dt.now().date())

    for excel_table in excel_tables:
        # excel_table = "昊居签约数据"

        print(f"read {excel_table}")
        df = pd.read_excel(io=file, sheet_name=excel_table)
        #插入更新时间
        df["更新时间"] = update_time

        # 修改列名
        renames = cols[excel_table]
        df: pd.DataFrame = df.rename_axis(mapper=renames, axis=1)

        df = df.replace("\s+-\s+", "", regex=True)


        sql_table = tables[excel_table]
        print(f"write {sql_table}")
        df.to_sql(name=sql_table,con=engine,index=False,if_exists="replace")

import os
if __name__=="__main__":
       pass


