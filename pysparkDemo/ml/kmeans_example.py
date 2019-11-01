#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/9/30 15:29
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
"""
K-means 算法也是一个迭代式的算法，其主要步骤如下:

第一步，选择 K 个点作为初始聚类中心。
第二步，计算其余所有点到聚类中心的距离，并把每个点划分到离它最近的聚类中心所在的聚类中去。
在这里，衡量距离一般有多个函数可以选择，最常用的是欧几里得距离 (Euclidean Distance), 也叫欧式距离。公式如下：
D(C,X)=(∑(ci-xi)^2)^1/2

其中 C 代表中心点，X 代表任意一个非中心点。

第三步，重新计算每个聚类中所有点的平均值，并将其作为新的聚类中心点。
最后，重复 (二)，(三) 步的过程，直至聚类中心不再发生改变，或者算法达到预定的迭代次数，又或聚类中心的改变小于预先设定的阀值。


在实际应用中，K-means 算法有两个不得不面对并且克服的问题:

1.聚类个数 K 的选择。K 的选择是一个比较有学问和讲究的步骤，我们会在后文专门描述如何使用 Spark 提供的工具选择 K。
2.初始聚类中心点的选择。选择不同的聚类中心可能导致聚类结果的差异。
"""

spark=SparkSession.builder.appName("k-means").master("local[3]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# co_cust=spark.read.format("delta").load("E:\\test\delta\\bronze\\DB2_DB2INST1_CO_CUST")
# KMeans().setFeaturesCol()


dataset = spark.read.format("libsvm").load("E:\github\spark\data/mllib/sample_kmeans_data.txt")

dataset.printSchema()
dataset.show(truncate=False)
kmeans=KMeans().setK(2).setSeed(1)
model=kmeans.fit(dataset)

predictions=model.transform(dataset)

predictions.show()

evaluator=ClusteringEvaluator()

silhouette=evaluator.evaluate(predictions)

print("Silhouette with squared euclidean distance = " + str(silhouette))


# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)