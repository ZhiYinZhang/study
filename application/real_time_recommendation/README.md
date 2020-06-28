# 实时推荐系统数据生成

## 项目目录结构
>real_time_recommendation
>>log                 ----------存储程序运行日志目录  
>>combine_data.py     ----------合并凤凰云和电影评分数据程序  
>>compact_table.py    ----------压缩delta表数据  
>>config.py           ----------配置参数  
>>generate_rating.py  ----------向kafka生成数据程序  
>>kafka_to_delta.py   ----------消费kafka数据写入delta表  
>>utils.py            ----------工具包  
>>run.py              ----------项目管理程序

## 环境依赖
>spark,Delta lake,kafka

## 数据流程
generate_rating.py脚本使用spark生成流式的数据写入kafka的topic，
kafka_to_delta.py脚本使用spark持续消费kafka的topic数据，然后spark
将消费的数据写入delta格式的目录下，供下游算法使用。  

## 发布流程
>1.修改配置  
根据环境修改config.py里面的配置,并根据配置创建对应的kafka的topic和hdfs目录  
>2.运行程序  
python run.py --start all  
这句命令会先向spark提交generate_rating.py脚本,向kafka的topic
里面写数据;  
然后会提交kafka_to_delta.py脚本，消费kafka topic的数据写入delta表


## 管理脚本
可以使用python run.py --help查看更多命令  

generate : generate_rating.py生产数据程序,向kafka里面生产数据    
save : kafka_to_delta.py保存数据程序,消费kafka并将数据保存到delta lake   
all : generate和save都执行 

optional arguments:  
  -h, --help            show this help message and exit  
  --start [generate|save|all]
                        启动对应的程序  
  --stop [generate|save|all]
                        停止对应的程序  
  --status [generate|save|all]
                        查看对应的程序的状态