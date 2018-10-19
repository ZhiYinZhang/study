#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/9/29 11:56

#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/9/28 11:59
import requests
import json

url = 'http://139.199.161.144:8080/ai/bigData/top/data/send'
body = {"instanceId" : "03AFA6B46592E56EDC0A2771FE547A4D","token":"6E113FB3561FE5A6F522F31FBA75E7FF"}
headers = {'content-type': "application/json", 'Authorization': 'APP appid = 4abf1a,token = 9480295ab2e2eddb8'}

# print type(body)
# print type(json.dumps(body))
# 这里有个细节，如果body需要json形式的话，需要做处理
# 可以是data = json.dumps(body)
response = requests.post(url, data=body)
# 也可以直接将data字段换成json字段，2.4.3版本之后支持
# response  = requests.post(url, json = body, headers = headers)

# 返回信息
print(response.text)
# 返回响应头
print(response.status_code)
