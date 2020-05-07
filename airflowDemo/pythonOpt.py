#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/12/25 14:27
from datetime import datetime,timedelta
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator


args={
    "owner":"airflow",
    "depends_on_past":False,
    "start_date":datetime(2020,1,3,14,20,0)
}
#定义一个DAG
dag=DAG(dag_id="pythonOpt",
        default_args=args,
        catchup=False,
        schedule_interval=timedelta(seconds=10)
        )

from pprint import pprint
def print_context(ds,**kwargs):
    pprint(kwargs)
    print(ds)
    return "whatever you return gets printed in the logs"

t1=PythonOperator(task_id="print_context",
               provide_context=True,
               python_callable=print_context,
               dag=dag
               )

def print_time():
    print(f"pythoOpt end {datetime.now()}")
t2=PythonOperator(task_id="end",
               python_callable=print_time,
               dag=dag
               )

t1 >> t2