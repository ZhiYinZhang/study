## 烟草零售户画像/区域画像/预警统计规则
### 环境:
    python3.6
### python依赖:
    happybase-1.1.0
    pandas-0.23.4
    pyspark-2.4.0
    apcheduler-3.6.0
### 启动的服务
    开启hbase的thrift服务
    cd {hbase_home}/bin
    hbase-daemon.sh start thrift
### 依赖的数据:
    1.在config.py修改hbase配置
    2.根据config.py，在hdfs上创建对应外部数据目录，并上传外部数据
    3.配置中间数据的目录
### jar包依赖
    将{phoenix_home}/phoenix-4.14.2-HBase-1.2-client.jar拷贝到{spark_home}/jars
    由于目前没有部署spark集群，只是配置了spark环境，还需将该包上传到hdfs的/user/entrobus/spark_jars
### 烟草集群部署:
    cd tobacco_rules
    zip -r dpd.zip ml/ rules/
    nohup python run.py >/dev/null 2>&1 &
    
    
