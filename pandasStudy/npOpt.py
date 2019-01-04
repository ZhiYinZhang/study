#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/4 14:58
import numpy as np
from matplotlib import pyplot as plt
x = np.arange(0,3*np.pi,0.1)

y_sin = np.sin(x)
y_cos = np.cos(x)

plt.plot(x,y_sin,color='red',linewidth=2.5,linestyle=".")
plt.plot(x,y_cos,color='blue',linewidth=5,linestyle="-")

plt.hist
