#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/11/7 14:26
import json,requests,textwrap,pprint,datetime
from requests.models import Response
import time
import sys
import random
from  operator import *
def create(param:dict):
    url = livy_host+"/sessions"
    rp = requests.post(url=url,data=json.dumps(param),headers=headers)
    pprint.pprint(rp.json())
    return rp
def submit(code:str,session_url:str):
    """

    :param code: pyspark代码块
    :param session_url: /sessions/{sessionId}
    :return:
    """
    url = livy_host+session_url+"/statements"
    code = textwrap.dedent(f"{code}")
    py_code = {"kind":"pyspark","code":code}
    rp = requests.post(url=url,data=json.dumps(py_code),headers=headers)
    pprint.pprint(rp.json())
    return rp
def get_status(location:str):
    # 查看spark应用状态  GET livy_host/sessions/{sessionId}
    # 查看程序运行状态   GET livy_host/sessions/{sessionId}/statements/{statementId}
    if location:
        url = livy_host+location
        result = requests.get(url=url,headers=headers)

        return result.json()
    else:
        pprint.pprint(location)

def get_sessions():
    response = requests.get(url=livy_host+'/sessions',headers=headers)
    sessions = {}
    idle = []
    busy = []
    for session in response.json()['sessions']:
        state = session['state']
        if state == 'idle':
            idle.append(session['id'])
        else:
            busy.append(session['id'])
    sessions['idle'] = idle
    sessions['busy'] = busy
    return sessions
def get_wait_statements(sessionId):
    url = livy_host+f"/sessions/{sessionId}/statements"
    response = requests.get(url=url)
    statements = response.json()['statements']
    wait = 0
    for i in statements:
        if i['state'] == 'waiting':
            wait+=1
    return wait

livy_host = "http://10.18.0.28:8998"
headers = {"Content-Type":"application/json"}
param = {
    "pyFiles":["hdfs://Entrobus11:8020/user/zhangzy/DPT/lib.zip"],
    "executorMemory":"4G",
    "executorCores":2,
    "numExecutors":2,
}
if __name__=="__main__":


    # param_path = sys.argv[1]


    #要执行的代码
    code = """
    import json
    import time
    import traceback as tb
    #要依赖的模块
    sc.addPyFile("hdfs://Entrobus11:8020/user/zhangzy/DPT/lib.zip")
    from lib.OperateFactory import OperateFactory
    from lib.utils.fileOperate import *
    from lib.utils import write2mq,del_expired,hdfsClient

    data = {'data': {'expId': '1EE16195AD6C7F50F84A47F1E4E822F2'},
     'headers': {'code': 0,
      'expId': '1EE16195AD6C7F50F84A47F1E4E822F2',
      'identifier': 'exp_execute_all',
      'msg': '',
      'sessionId': 'BE08BDEB463A7AFCA023BE4CCD65C7F5',
      'userId': '21C5BCE840B3AE9170C5A69EF62B40E9'}
    }
    userId = data['headers']['userId']
    expId = data['headers']['expId']
    sessionId = data['headers']['sessionId']

    cli = hdfsClient.get_hdfs_Client()
    # with cli.read(f"temp/{userId}/{expId}/{sessionId}.json") as reader:
    #     data_args = json.load(reader)
    with cli.read(f"test/model01.json") as reader:
        data_args = json.load(reader)
    
    print(f"消息{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())}:{data_args}")
    #删除用户目录下过期实验数据
    header = data_args['headers']
    del_expired.del_expire_data(header)
    #删除游客
    if re.findall(r'^(Tourist)',header['userId']):
        del_expired.del_tourist(header)
    #将csv结尾换成parquet
    for operation in data_args['process']:
        if operation['action'] == 'dataset':
            dataset = operation['outputDatasets'][0]
            dataset = dataset.split(".")[0]+".parquet"
            # 工作流列表
            #filePath = os.path.join("e:/pythonProject", "dataset",dataset)
            filePath = f"DPT/dataset/{dataset}"
            operation["filePath"] = filePath
    workflow = data_args["process"]
    # 元素名与实际对象的映射
    name_operation = {}
    flag = True

    i = 0
    print('准备进入workflow')
    for step in workflow:
        print(i,step['action'])
        i += 1
        try:
            # 依工作流次序执行操作
            operation_instance = OperateFactory.initial(step, name_operation, spark)
            if flag:
                operation_instance.execute()
                if step.get("end_execute_node"):
                    flag = False
                step["code"] = 0
                step["msg"] = "success"
                if step.get("result", "") == "":
                    step["result"] = ""
            else:
                step["code"] = 2
                step["msg"] = "not run"
                step["result"] = ""
        except Exception as e:
            exc_info = tb.format_exc()
            print(exc_info)

            # 返回码  成功：0 失败：1
            step["code"] = 1
            step["msg"] = "参数错误"
            step["result"] = ""

            data_args["headers"]["code"] = 1
            data_args["headers"]["msg"] = "参数错误"
            flag = False

    # 本次试验所有的中间数据和结果的存储路径   userId/实验id/
    file_path = f"temp/{data_args['headers']['userId']}/{data_args['headers']['expId']}/"

    #必须在spark.stop()前执行
    start = time.time()
    write(name_operation=name_operation,file_path=file_path)
    end = time.time()
    print(f"总用时:{end-start}s")

    # 供调试用，结果显示在SparkHistory Driver的stdout中
    print("the result is:")
    # 定义写回RabbitMQ的字典
    result = {
        "data": data_args["data"],
        "headers": data_args["headers"],
        "process": data_args["process"]
    }
    # 写回结果到mq
    # write2mq.write(result)
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())}:{result}")
    print("spark stop")
    """
    # # 创建spark应用
    # for i in range(10):
    #     rp = create(param)
    #     # /sessions/{sessionId}
    #     session = rp.headers.get('location')
    #     print(session)

    # sessions = get_sessions()
    # if len(sessions['idle'])>0:
    #     #提交
    #     rp = submit(code=code,session_url=f"/sessions/{sessions['idle'][0]}")
    # else:
    #     busys = sessions['busy']
    #     #获取等待的job最少的一个session
    #     if len(busys) > 0:
    #         #    (sessionId,waitNum)
    #         sessionId_waitNum = []
    #         for busy in busys:
    #             waitNum = get_wait_statements(busy)
    #             sessionId_waitNum.append((busy, waitNum))
    #         print(sessionId_waitNum)
    #         sort = sorted(sessionId_waitNum, key=lambda x: x[1])
    #         sessionId = sort[0][0]
    #         rp = submit(code=code,session_url=f"/sessions/{sessionId}")

    #
    # statement = rp.headers.get("location")
    # start = datetime.datetime.now()
    # #遍历获取程序运行progress
    # flag = True
    # while flag:
    #     time.sleep(1)
    #     result = get_status(statement)
    #     if result['state']=="available":
    #         flag = False
    #     print(f"progress:{result['progress']}")
    # end = datetime.datetime.now()
    # pprint.pprint(result)
    # print(f"所用时间:{end-start}")

    sessions = get_sessions()
    busy_num = len(sessions['busy'])
    idle_num = len(sessions['idle'])
    total_session = busy_num + idle_num
    print(f"total sessions:{total_session}")
    # if total_session
