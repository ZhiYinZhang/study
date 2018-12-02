#-*- coding: utf-8 -*-
# @Time    : 2018/3/28 16:31
# @Author  : Z
# @Email   : S
# @File    : objectTest.py



class car:
    #类似于java构造方法
    def __init__(self,color,wheelNum):
        self.color=color
        self.wheelNum=wheelNum
    #定义了该方法  创建对象时  会打印该方法返回的值
    def __str__(self):
        return '[color:'+self.color+',wheelNum:'+str(self.wheelNum)+']'
    def move(self):
        print ('一辆',self.color,'的汽车在跑')

#self 当前对象的应用

BMW=car('block',4)



# BMW.move()
# print BMW.wheelNum
print (BMW)


