#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/12/30 14:24
from airflow.models import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime,timedelta

args={
    "owner":"airflow",
    "depends_on_past":False,
    "start_date":datetime(2019,12,30)
}
#定义一个DAG
dag=DAG(dag_id="sshOpt",default_args=args,schedule_interval="* 15 * * * */10",catchup=False)


t=SSHOperator(ssh_conn_id="ssh_entrobus34",#指定conn id
            task_id="ssh_other_host",
            command="date >> /home/zhangzy/pyproject/test.log",
            dag=dag
              )