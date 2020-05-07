#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 14:59
import jpype
def getMaxMemory():
    # 获取默认jvm路径
    jvmPath = jpype.getDefaultJVMPath()
    print(jvmPath)
    if not jpype.isJVMStarted():
        # 启动jvm
        jpype.startJVM(jvmPath)
    # 调用java打印程序
    jpype.java.lang.System.out.println("hello world")
    # 调用lang包下的Runtime
    maxMemory = jpype.java.lang.Runtime.getRuntime().maxMemory()
    print(maxMemory)

    # jpype.shutdownJVM()
import datetime
from dateutil.relativedelta import relativedelta
from dateutil import rrule

if __name__=="__main__":
    import sys

    encode=sys.getdefaultencoding()
    print(encode)

