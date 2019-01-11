#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2019/1/9 16:00
from thrift import Thrift
from thrift.transport import TSocket,TTransport
from thrift.protocol import TBinaryProtocol


transport=TSocket.TSocket("entrobus28",9090)

transport.setTimeout(5000)
#传输方式 TFramedTransport/TBufferTransport
tans = TTransport.TBufferedTransport(transport)
#传输协议
