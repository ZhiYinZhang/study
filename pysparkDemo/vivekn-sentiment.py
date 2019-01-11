#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/11 11:56
#Imports#Import
import time
import sys
import os

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel
from sparknlp.annotator import *
from sparknlp.base import DocumentAssembler, Finisher

spark = SparkSession.builder \
    .appName("VivekNarayanSentimentApproach")\
    .master("local[*]")\
    .config("spark.driver.memory","8G")\
    .config("spark.driver.maxResultSize", "2G")\
    .config("spark.jar", "E://spark-nlp_2.11-1.8.0.jar")\
    .config("spark.kryoserializer.buffer.max", "500m")\
    .getOrCreate()