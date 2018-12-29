#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/25 16:46
from pyspark.sql import SparkSession,DataFrame
from pyspark import SparkContext
from pyspark.sql.functions import *

spark = SparkSession.builder \
                    .appName("loanRisk") \
                    .master("local[2]") \
                    .getOrCreate()
sc:SparkContext = spark.sparkContext
sc.setLogLevel("WARN")

file_path = "E:\资料\ml\ML-At-Scale-eBook-181029\Lending_Club_Loan_Data\lending-club-loan-data\loan.csv"

loan_stats = spark.read \
     .format("csv") \
     .option("header","true") \
     .option("inferSchema","true") \
     .load(file_path)

loan_stats.printSchema()
loan_stats.loan_status.isin

#将loan_status不在["Default","Charged Off","Fully Paid"]中的过滤，
loan_stats = loan_stats.filter( \
    loan_stats.loan_status.isin( \
        ["Default","Charged Off","Fully Paid"]
        )\
    ).withColumn(
        "bad_loan",
         (~(loan_stats.loan_status == "Fully Paid")
          ).cast("string")
    )

loan_stats.printSchema()
loan_stats.groupBy("addr_state").agg((count(col("annual_inc"))).alias("ratio")).show(truncate=False)