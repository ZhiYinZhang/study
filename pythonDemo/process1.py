#!/usr/bin/env python
# -*- coding:utf-8 -*-
from pyspark.sql import SparkSession,DataFrame,Row,Window,functions
from pyspark.ml.linalg import Vectors
from pyspark import SparkJobInfo
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import subprocess as sp


s1=sp.Popen("dirs",shell=True,)
s2=sp.Popen(executable="dirs",shell=True)
print(type(s2))
print(s2)

s1.kill()
print(s1.pid)