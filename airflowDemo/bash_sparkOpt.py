#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/3 9:59
from datetime import datetime as dt,timedelta
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

args={
    "owner":"airflow",
    "depends_on_past":False,
    "start_date":dt(2020,1,3,10,10,00)
}


#定义一个DAG
dag=DAG(dag_id="bash_sparkOpt",default_args=args,schedule_interval="* 15 * * 5 */30",catchup=False)

cmd="""
spark2-submit --class org.apache.spark.examples.SparkPi \
--name 8888 \
--master yarn \
--deploy-mode client \
--executor-memory 10g \
--num-executors 5 \
--executor-cores 5 \
/opt/cloudera/parcels/SPARK2/lib/spark2/examples/jars/spark-examples_2.11-2.4.0.cloudera2.jar \
10
"""

t1=BashOperator(task_id="t1",bash_command="date;sleep 10",dag=dag)
t2=BashOperator(task_id="spark",bash_command=cmd,dag=dag)
t3=BashOperator(task_id="t3",bash_command="date",dag=dag)

t1>>t2>>t3