#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/1/7 17:22
from datetime import datetime,timedelta
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
args={
    "owner":"airflow",
    "depends_on_past":False,
    "start_date":datetime(2019,12,25)
}


#定义一个DAG
dag=DAG(dag_id="jinjaOpt",default_args=args,schedule_interval=timedelta(seconds=30),catchup=False)

print(dag.start_date)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7) }}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t1 = BashOperator(
    task_id='t1',
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)