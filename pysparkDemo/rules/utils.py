#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/4/8 16:39
from pyspark.sql.functions import udf,date_trunc,datediff,col
def period(x,y):
    try:
        x=float(x)
        y=float(y)
        return round((x-y)/y,2)
    except Exception as e:
        print(e.args)
        print(x,y)
period_udf = udf(period)


def divider(x,y):
    try:
        return round((x/y),2)
    except Exception as e:
        print(e.args)
        print(x,y)
divider_udf=udf(divider)


def month_diff(year,month,today_year,today_month):
    db=today_year-year
    month_diff=(today_month+db*12)-month
    return month_diff

month_diff_udf=udf(month_diff)


def week_diff0(year, month, week, today_year, today_month, today_week):
    db = today_year - year
    # 因为2018-12-31与2019-01-01 的 weekofyear都是1，db要减去1
    if week == 1 and month != today_month:
        db -= 1
    week_diff = (today_week + 52 * db) - week
    return week_diff

week_diff_udf=udf(week_diff0)


def week_diff(end,start):
    # date_trunc("week",date) 返回 所属周的周一的日期
    return datediff(date_trunc("week", col(end)), date_trunc("week", col(start))) / 7



if __name__=="__main__":

    print(period(str(95.47),str(0.0000)))