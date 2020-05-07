#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/12/25 14:23
from datetime import datetime,timedelta
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
args={
    "owner":"airflow",
    "depends_on_past":False,
    "start_date":days_ago(0)
}



#定义一个DAG
dag=DAG(dag_id="shellOpt",default_args=args,schedule_interval=timedelta(seconds=5),catchup=False)

dag=DAG(dag_id="shellOpt",default_args=args,schedule_interval="* * * * * *",catchup=False)
#shell操作
t1=BashOperator(task_id="a",bash_command="echo a",dag=dag)

