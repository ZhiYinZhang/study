#-*- coding: utf-8 -*-
# @Time    : 2018/10/21 23:02
# @Author  : Z
# @Email   : S
# @File    : sort_DAG.py

import json
import time
def parse_DAG(data):
    #所有操作
    process = data['process']

    #排好序后的操作
    process_sort = []
    #将输出没有作为过输入的操作，即最后的操作，添加到process_sort
    for action1 in process:
        flag = True
        for action2 in process:
            if action1['outputDatasets'][0] in action2['inputDatasets']:
                     flag = False
        #flag为True:该操作的输出没有作为过输出，是最后的操作
        if flag:
            process_sort.append(action1)

    #将与最后一个操作相关的操作添加进来，即一直向上追溯，直到第一个
    for s in process_sort:
       #遍历操作s的输入
       for input in s['inputDatasets']:
           #遍历所有操作，寻找操作s的上一级
           for action in process:
               #input在action的输出中存在，说明操作action是操作s的上一级
               if input in action['outputDatasets']:
                   #一、只有一个最后操作的时候，为了避免有某两个操作的输入依赖同一个操作，出现重复添加
                   #二、在有多个最后操作的时候，避免重复添加
                   if action not in process_sort:
                       process_sort.append(action)
    process_sort.reverse()
    data['process'] = process_sort
    return data
if __name__=="__main__":
    data = {
    "data": {
        "expId": "E3204CA4A11B1F72E3768CC314190150"
    },
    "headers": {
        "code": 0,
        "expId": "E3204CA4A11B1F72E3768CC314190150",
        "identifier": "exp_execute_all",
        "msg": "",
        "sessionId": "737592BFDA77CA7C55F3C7BE65655D4C",
        "userId": "21C5BCE840B3AE9170C5A69EF62B40E9"
    },
    "process": [
        {
            "action": "dataset",
            "inputDatasets": [

            ],
            "outputDatasets": [
                "model02_train.csv"
            ]
        },
        {
            "action": "typetransform",
            "inputDatasets": [
                "model02_train.csv"
            ],
            "outputDatasets": [
                "typetransform_1_Output_1"
            ]
        },
		{
            "action": "predict",
            "inputDatasets": [
                "RF_model_1_Output_1",
                "sampling_2_Output_1"
            ],
            "outputDatasets": [
                "predict_2_Output_1"
            ]
        },
        {
            "action": "sampling",
            "inputDatasets": [
                "typetransform_1_Output_1"
            ],
            "outputDatasets": [
                "sampling_1_Output_1"
            ]
        },
		{
            "action": "predict",
            "inputDatasets": [
                "RF_model_1_Output_1",
                "sampling_2_Output_1"
            ],
            "outputDatasets": [
                "predict_1_Output_1"
            ]
        },
        {
            "outputDatasets": [
                "RF_model_1_Output_1"
            ],
            "action": "train",
            "inputDatasets": [
                "sampling_1_Output_1"
            ]
        },
        {
            "action": "sampling",
            "inputDatasets": [
                "typetransform_1_Output_1"
            ],
            "outputDatasets": [
                "sampling_2_Output_1"
            ]
        }
    ]
}
    # with open("e:/javacode/test.json", 'r') as file:
    #     data = json.load(file)
    parse_DAG(data)
    print(data)