#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/8/26 16:47
import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
df=pd.DataFrame(np.random.randint(0,10,10),columns=["a"])
df1=pd.DataFrame(np.random.randn(100,3),columns=["a","b","c"])


#更多参数   df1.plot -> plot = CachedAccessor("plot", gfx.FramePlotMethods)
        # ->  gfx.FramePlotMethods   ->  FramePlotMethods的__call__方法返回的对象 plot_frame
# help(df1.plot.__call__)




#直接使用plot  得到每列的折线图
# df1.plot()
# plt.show()


"""
更多的参数  ctrl+左键  选择参数做多的那个
"""


# bar或barh 柱状图
# df1[["a","b"]].plot.bar(figsize=[5,5],title="aaaaa")
# plt.show()

#散点图
# ax=df1.plot.scatter(x="a",y="b",color="LightGreen",label="class1")
#将这个画布与上一个放在一起
# df1.plot.scatter(x="a",y="c",color="DarkBlue",label="class1",ax=ax)
# plt.show()





print(df)



# -----频率直方图 用来统计某列的值 在各个范围内的数量  [,)
# ax=df.plot.hist(bins=[0,2,4,6,8,10],histtype="bar",color="pink")
#
#
# #解决 横纵坐标中文乱码
# mpl.rcParams["font.sans-serif"]=['Microsoft YaHei']
# # 解决保存图像是负号'-'显示为方块的问题
# mpl.rcParams['axes.unicode_minus'] = False
#
# #设置横纵坐标
# ax.set_xlabel("横坐标")
# ax.set_ylabel("纵坐标")
#
# # help(ax)
# plt.show()



#-----饼图   展示某列各个值的占比
# df.plot.pie(y="a")
# plt.show()


# class 里面的值 指的是 value的每个值的含义
# df2 = pd.DataFrame({'value': [0.330, 4.87 , 5.97],
#                     "class":["class1","class2","class3"]})
#
# print(df2)
# df2.plot.pie(y="value",labels=df2["class"])
#
# plt.show()



