#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/3/18 10:29
from pykafka import KafkaClient
from pykafka.topic import Topic
from pykafka.partition import Partition
from pykafka.protocol import Message,OffsetPartitionResponse

client=KafkaClient("10.18.0.32:9092")

topic:Topic=client.topics[b"test"]



#查看该topic分区,{分区id:pykafka.partition.Partition实例}
partitions=topic.partitions
print(partitions)
for i in range(len(partitions)):
    partition:Partition=partitions[i]
    # print(partition.topic)
    # print(partition.earliest_available_offset())
    # print(partition.latest_available_offset())
    print(partition.id)
    #pykafka.broker.Broker:<pykafka.broker.Broker at 0x37e1518 (host=b'entrobus32', port=9092, id=125)>
    print(partition.leader)
    #[pykafka.broker.Broker]
    print(partition.isr)
    #[pykafka.broker.Broker]
    print(partition.replicas)



#查看各分区最早的可用offset：{分区id:pykafka.protocol.OffsetPartitionResponse实例}
earliest_offsets=topic.earliest_available_offsets()
print(earliest_offsets,type(earliest_offsets[0]))

#查看各分区最新的可用offset：{分区id:pykafka.protocol.OffsetPartitionResponse实例}
latest_offsets=topic.latest_available_offsets()
print(latest_offsets,type(latest_offsets[0]))
