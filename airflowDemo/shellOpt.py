#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/12/25 14:23
from datetime import datetime,timedelta
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

args={
    "owner":"airflow",
    "depends_on_past":False,
    "start_date":datetime(2019,12,25)
}
#定义一个DAG
dag=DAG("test",default_args=args,schedule_interval=timedelta(seconds=5))

#shell操作
t1=BashOperator(task_id="a",bash_command="echo a",dag=dag)

#python操作
t2=PythonOperator(task_id="b",python_callable="print()")