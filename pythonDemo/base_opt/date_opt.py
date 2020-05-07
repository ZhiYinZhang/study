#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/3/23 11:39
from dateutil.parser import parse
from dateutil import rrule
from dateutil import relativedelta as rd
from dateutil.relativedelta import relativedelta
from datetime import datetime as dt
#-----parser 将字符串解析成datetime,自动匹配任意格式
print(parse("2020/03/11"))
print(parse("2020-01-11"))
print(parse("20200203"))

print(parse("3/11")) #默认今年，解析月-日
# print(parse("14/11")) #默认今年，解析月-日，超过12会报错

#当年在后面时，并第一个数符合月的大小时，解析成：月-日-年
print(parse("03-02-2019"))
#当超过12时，会识别成 日-月-年
print(parse("15-02-2019"))
#但当年在前面时，只能按 年-月-日顺序，这个会报错
# print(parse("2019-15-02"))

print(parse("Mar 12"))




#-----rrule  根据规则生成datetime
"""
rrule(self, freq, dtstart=None, interval=1, wkst=None,count=None, until=None,
 bysetpos=None,bymonth=None, bymonthday=None, byyearday=None, byeaster=None,
 byweekno=None, byweekday=None, byhour=None, byminute=None, bysecond=None,cache=False)
 
 freq:单位，rrule.YEARLY rrule.MONTHLY rrule.WEEKLY rrule.DAILY rrule.HOURLY rrule.MINUTELY rrule.SECONDLY
 dtstart:开始时间
 until:结束时间
 wkst:周开始时间
 interval:间隔
 count:个数
 byxxx:指定匹配的周期，byweekday=(MO,TU)则只有周一，周二匹配
"""
r=list(rrule.rrule(freq=rrule.DAILY,dtstart=parse("20200323"),until=parse("20200329"),byweekday=(rrule.MO,rrule.TU)))
print(r)

#-----relativedelta 计算时间差
"""
        year, month, day, hour, minute, second, microsecond:
           将对应的替换为这个值
        years, months, weeks, days, hours, minutes, seconds, microseconds:
            加上/减去这个值，优先级比上面低，先替换再加减
        weekday:
            将日期替换为下几个 或 上几个 星期几
            from dateutil import relativedelta as rd
            rd.MO(+1),下一个周一
            rd.MO(-1)，上一个周一

        leapdays:
            Will add given days to the date found, if year is a leap
            year, and the date found is post 28 of february.

        yearday, nlyearday:
            Set the yearday or the non-leap year day (jump leap days).
            These are converted to day/month/leapdays information.
"""
r2=relativedelta(parse("20200310"),parse("20200201"))
print(r2)


r3=parse("20200101")-relativedelta(months=1)
print(r3)


r4=parse("20200330")-relativedelta(months=1)
#2020-02-29 00:00:00, 30超过2月的日期，会自动这只为最后一天
print(r4)

#先用month替换，再减去months
r5=parse("20200330")-relativedelta(months=1,month=5)
print(r5)


#获取下一个周一
r6=parse("20200324")+relativedelta(weekday=rd.MO(+1))
r7=parse("20200324")-relativedelta(weekday=rd.MO(+1))
#与日期是加上还是减去relativedelta没有关系
print(r6,r7)

#获取上一个周一
r8=parse("20200324")+relativedelta(weekday=rd.MO(-1))
print(r8)


#day超过，会默认给最后一天；其他的year，month等会报错
r9=parse("20200323")+relativedelta(day=40)
print(r9)