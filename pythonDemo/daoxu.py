#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/9/28 11:59
import requests
import json
import uuid

data = {
    "data": {
        "expId": "470452BE2A31E750E8A41A177C46FB1E"
    },
    "headers": {
        "code": 0,
        "expId": "470452BE2A31E750E8A41A177C46FB1E",
        "identifier": "exp_execute_all",
        "msg": "",
        "sessionId": "82C3408E1CC06DFA4142E200FDDA214A",
        "userId": ""
    },
    "process": [
        {
            "action": "dataset",
            "inputDatasets": [],
            "outputDatasets": [
                "model04_train_02.csv"
            ]
        },
        {
            "action": "typetransform",
            "inputDatasets": [
                "model04_train_02.csv"
            ],
            "outputDatasets": [
                "typetransform_1_Output_1"
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
            "action": "sampling",
            "inputDatasets": [
                "typetransform_1_Output_1"
            ],
            "outputDatasets": [
                "sampling_3_Output_1"
            ]
        },
        {
            "outputDatasets": [
                "GBDT_model_1_Output_1"
            ],
            # "end_execute_node":"1",
            "action": "train",
            "inputDatasets": [
                "sampling_1_Output_1"
            ]
        },
        {
            "action": "sampling",
            "inputDatasets": [
                "sampling_3_Output_1"
            ],
            "outputDatasets": [
                "sampling_4_Output_1"
            ]
        },
        {
            "action": "sampling",
            "inputDatasets": [
                "sampling_1_Output_1"
            ],
            "outputDatasets": [
                "sampling_2_Output_1"
            ]
        },
        {
            "action": "train",
            "inputDatasets": [
                "sampling_3_Output_1"
            ],
            "outputDatasets": [
                "SVM_model_1_Output_1"
            ]
        },
        {
            "action": "predict1",
            "inputDatasets": [
                "GBDT_model_1_Output_1",
                "sampling_2_Output_1"
            ],
            "outputDatasets": [
                "predict_1_Output_1"
            ]
        },
        {
            "action": "predict2",
            "inputDatasets": [
                "sampling_4_Output_1",
                "SVM_model_1_Output_1"
            ],
            "outputDatasets": [
                "predict_2_Output_1"
            ],
        }
    ],
    "outElem": [
        "predict_1_Output_1",
        "predict_2_Output_1"
    ]
}
"""
{'action': 'dataset', 'inputDatasets': [], 'outputDatasets': ['model04_train_02.csv']}
{'action': 'typetransform', 'inputDatasets': ['model04_train_02.csv'], 'outputDatasets': ['typetransform_1_Output_1']}
{'action': 'sampling', 'inputDatasets': ['typetransform_1_Output_1'], 'outputDatasets': ['sampling_1_Output_1']}
{'action': 'sampling', 'inputDatasets': ['typetransform_1_Output_1'], 'outputDatasets': ['sampling_3_Output_1']}
{'outputDatasets': ['GBDT_model_1_Output_1'], 'end_execute_node': '1', 'action': 'train', 'inputDatasets': ['sampling_1_Output_1']}
"""

process= data['process']
flag = True
#判断是否有'end_execute_node'
for key in process:
    if key.get('end_execute_node'):
        #有设为True
        flag = True
if flag:
    #end_execute_node之前的所有操作，包括自己
    front_all=[]
    for key in process:
        print(key)
        front_all.append(key)
        if key.get('end_execute_node'):
            break
    print('-----------------------------------------------------')

    single_line = []
    #将标为'end_execute_node'的操作添加到single_line
    single_line.append(front_all[-1])
    # d1.remove(d1[-1])
    for i in single_line:
        #遍历操作的inputDatasets
        for input in i["inputDatasets"]:
            #遍历d1
            for action in front_all:
                if input in action['outputDatasets']:
                       print(action)
                       single_line.append(action)
                       front_all.remove(action)

    print('-----------------------------------------------------')
    single_line.reverse()
    for i in single_line:
          process.remove(i)
    single_line.extend(process)
    data['process'] = single_line
print(data)