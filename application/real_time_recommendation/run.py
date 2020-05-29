#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/5/18 12:37
import argparse
from datetime import datetime as dt
import subprocess as sp
import traceback as tb

m={"generate":"generate_rating","save":"kafka_to_delta"}

def start_program(cmd):
    stat=status_program(cmd)
    if stat is None:
        prog = m[cmd]

        date_str=dt.now().strftime("%Y%m%d_%H%M%S")
        log_file=f"log/{prog}-{date_str}.log"

        command=f"""
nohup \
spark2-submit --name {prog} \
--master yarn \
--deploy-mode client \
--num-executors 5 \
--executor-memory 5g \
--executor-cores 3 \
--py-files config.py \
--conf spark.executor.memoryOverhead=512m \
--conf spark.pyspark.python=/home/hadoop/anaconda3/bin/python \
{prog}.py \
>{log_file} 2>&1 &
        """
        print(command)
        sp.Popen(command,shell=True)
        print(f"{cmd} start                  [  ok  ]")
    else:
        print(f"{cmd} is running")
def stop_program(cmd):
    stat=status_program(cmd)
    if stat is not None:
        appId=stat.decode("utf-8").split("\t")[0]
        command=f"yarn application -kill {appId}"
        sp.Popen(command,shell=True)
        print(f"{cmd} stop                    [ ok  ]")
    else:
        print(f"{cmd} is not running")


def status_program(cmd):
    prog=m[cmd]
    command=f"yarn application -list|grep '{prog}'"
    try:
      r=sp.check_output(command,shell=True)
      return r
    except:
      return None
def compact(numFiles):
    prog="compact_table"
    date_str = dt.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"log/{prog}-{date_str}.log"

    command = f"""
    nohup \
    spark2-submit --name {prog} \
    --master yarn \
    --deploy-mode client \
    --num-executors 5 \
    --executor-memory 5g \
    --executor-cores 3 \
    --py-files config.py \
    --conf spark.executor.memoryOverhead=512m \
    --conf spark.pyspark.python=/home/hadoop/anaconda3/bin/python \
    {prog}.py {numFiles} \
    >{log_file} 2>&1 &
            """
    print(command)
    sp.Popen(command, shell=True)
def initial():
    pass
if __name__=="__main__":
  help_info = """
    generate:generate_rating.py生产数据程序,向kafka里面生产数据
    save:kafka_to_delta.py保存数据程序,消费kafka并将数据保存到delta lake
    all:generate和save都执行
    """
  parser=argparse.ArgumentParser(description=help_info,prog="real-time recommendation")
  #创建互斥组
  group=parser.add_mutually_exclusive_group()


  a=group.add_argument("--start","-start",
                     choices=["generate","save","all"],
                     nargs="?",
                     const="all",
                     help="启动对应的程序",
                     metavar="generate|save|all"
                     )
  group.add_argument("--stop","-stop",
                     choices=["generate","save","all"],
                     nargs="?",
                     const="all",
                     help="停止对应的程序",
                     metavar="generate|save|all"
                     )
  group.add_argument("--status","-status",
                     choices=["generate", "save", "all"],
                     nargs="?",
                     const="all",
                     help="查看对应的程序",
                     metavar="generate|save|all"
                     )
  group.add_argument("--compact","-compact",
                     type=int,
                     nargs="?",
                     const=100,
                     help="压缩文件,default:100",
                     metavar="numFiles"

                     )

  args=parser.parse_args()

  if args.start:
      start=args.start
      if start=="all":
            start_program("generate")
            start_program("save")
      else:
          start_program(start)
  if args.stop:
      stop=args.stop
      if stop=="all":
          stop_program("generate")
          stop_program("save")
      else:
          stop_program(stop)
  if args.status:
      status = args.status
      if status == "all":
          for c in m.keys():
              if status_program(c):
                  print(f"{c} is running")
              else:
                  print(f"{c} is not running")
      else:
          if status_program(status):
              print(f"{status} is running")
          else:
              print(f"{status} is not running")
  if args.compact:
      numFiles=args.compact
      compact(numFiles)