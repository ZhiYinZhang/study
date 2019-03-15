#-*- coding: utf-8 -*-
# @Time    : 2018/3/28 17:48
# @Author  : Z
# @Email   : S
# @File    : objectTest1.py


class person:
    def __init__(self,name):
        #__name表示私有属性
            self.__name=name
    def getName(self):
            return self.__name
    def setName(self,name):
        if len(self.__name)>0 and len(self.__name)<5:
               self.__name=name
        else:
            print('error:名字长度不能为空或超过5')
    def __del__(self):
        print(self.__name+'马上被干掉了')
    def __eat(self):
        print ('eat')

xiaoming=person('dongGe')
xiaoming.setName('wangwu')
# print xiaoming.getName()



class person1:
    country='china'
    def __init__(self,name):
            self.name=name
    def printName(self):
            print (self.name)

p1=person1('zhangsan')
p1.sex='man'
p1.country='england'
p2=person1('lisi')
p2.age=12
print (p1.sex)
print (p2.age)
print (p1.country)
print (p2.country)




