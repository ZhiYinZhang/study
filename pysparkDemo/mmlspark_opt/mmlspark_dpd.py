#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/5/28 15:21
import subprocess as sp
"""
先将mmlspark所有依赖包安装到maven本地仓库，
这样在build mmlspark时就不会去下载了

"""


urls=["https://mmlspark.azureedge.net/maven/com/microsoft/ml/spark/mmlspark_2.11/1.0.0-rc1/mmlspark_2.11-1.0.0-rc1.jar",
"http://maven.aliyun.com/nexus/content/groups/public/org/scalactic/scalactic_2.11/3.0.5/scalactic_2.11-3.0.5.jar",
"http://maven.aliyun.com/nexus/content/groups/public/org/scalatest/scalatest_2.11/3.0.5/scalatest_2.11-3.0.5.jar",
"http://maven.aliyun.com/nexus/content/groups/public/io/spray/spray-json_2.11/1.3.2/spray-json_2.11-1.3.2.jar",
"http://maven.aliyun.com/nexus/content/groups/public/com/microsoft/cntk/cntk/2.4/cntk-2.4.jar",
"http://maven.aliyun.com/nexus/content/groups/public/org/openpnp/opencv/3.2.0-1/opencv-3.2.0-1.jar",
"http://maven.aliyun.com/nexus/content/groups/public/com/jcraft/jsch/0.1.54/jsch-0.1.54.jar",
"http://maven.aliyun.com/nexus/content/groups/public/org/apache/httpcomponents/httpclient/4.5.6/httpclient-4.5.6.jar",
"http://maven.aliyun.com/nexus/content/groups/public/com/microsoft/ml/lightgbm/lightgbmlib/2.3.100/lightgbmlib-2.3.100.jar",
"http://maven.aliyun.com/nexus/content/groups/public/com/github/vowpalwabbit/vw-jni/8.7.0.3/vw-jni-8.7.0.3.jar",
"http://maven.aliyun.com/nexus/content/groups/public/org/scala-lang/scala-reflect/2.11.12/scala-reflect-2.11.12.jar",
"http://maven.aliyun.com/nexus/content/groups/public/org/scala-lang/modules/scala-xml_2.11/1.0.6/scala-xml_2.11-1.0.6.jar",
"http://maven.aliyun.com/nexus/content/groups/public/org/apache/httpcomponents/httpcore/4.4.10/httpcore-4.4.10.jar",
"http://maven.aliyun.com/nexus/content/groups/public/commons-logging/commons-logging/1.2/commons-logging-1.2.jar",
"http://maven.aliyun.com/nexus/content/groups/public/commons-codec/commons-codec/1.10/commons-codec-1.10.jar"]

def download_jar():
    #下载jar包到指定目录
    for url in urls:
        print(f"down {url} to {down_dir}")
        sp.check_call(f"wget -P {down_dir} {url}",shell=True)


jars=[
"com/microsoft/ml/spark/mmlspark_2.11/1.0.0-rc1/mmlspark_2.11-1.0.0-rc1.jar",
"org/scalactic/scalactic_2.11/3.0.5/scalactic_2.11-3.0.5.jar",
"org/scalatest/scalatest_2.11/3.0.5/scalatest_2.11-3.0.5.jar",
"io/spray/spray-json_2.11/1.3.2/spray-json_2.11-1.3.2.jar",
"com/microsoft/cntk/cntk/2.4/cntk-2.4.jar",
"org/openpnp/opencv/3.2.0-1/opencv-3.2.0-1.jar",
"com/jcraft/jsch/0.1.54/jsch-0.1.54.jar",
"org/apache/httpcomponents/httpclient/4.5.6/httpclient-4.5.6.jar",
"com/microsoft/ml/lightgbm/lightgbmlib/2.3.100/lightgbmlib-2.3.100.jar",
"com/github/vowpalwabbit/vw-jni/8.7.0.3/vw-jni-8.7.0.3.jar",
"org/scala-lang/scala-reflect/2.11.12/scala-reflect-2.11.12.jar",
"org/scala-lang/modules/scala-xml_2.11/1.0.6/scala-xml_2.11-1.0.6.jar",
"org/apache/httpcomponents/httpcore/4.4.10/httpcore-4.4.10.jar",
"commons-logging/commons-logging/1.2/commons-logging-1.2.jar",
"commons-codec/commons-codec/1.10/commons-codec-1.10.jar"
]
def install_jar():
    for url in jars:
        print(url)
        url_list = url.split("/")
        jar = url_list.pop()
        version = url_list.pop()
        artifactId = url_list.pop()
        groupId = ".".join(url_list)

        mvn = f"""
    mvn install:install-file \
    -Dfile={down_dir}/{jar} \
    -DgroupId={groupId} \
    -DartifactId={artifactId} \
    -Dversion={version} \
    -Dpackaging=jar
        """
        mvn = f"""
            mvn install:install-file ^
            -Dfile={down_dir}/{jar} ^
            -DgroupId={groupId} ^
            -DartifactId={artifactId} ^
            -Dversion={version} ^
            -Dpackaging=jar
                """
        print(mvn)
        sp.check_call(mvn, shell=True)
        print("安装成功！")

if __name__=="__main__":
    # down_dir = "/home/zhangzy/packages/mmlspark_dpd"
    down_dir = "E:/test/mmlspark/mmlspark_dpd"
    # download_jar()
    install_jar()