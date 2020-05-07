#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2020/4/30 18:14
def get_split(files):
    """
       1.先计算maxSplitBytes
       2.然后遍历目录下文件，
      每个文件按照maxSplitBytes去split，split多少份就多少个分区，不足maxSplitBytes的也作为一个分区
    """
    defaultMaxSplitBytes = 128 * 1024 * 1024
    openCostInBytes = 4 * 1024 * 1024
    defaultParallelism = 200

    totalBytes = sum((file + openCostInBytes for file in files))

    bytesPerCore = int(totalBytes / defaultParallelism)

    maxSplitBytes = min(defaultMaxSplitBytes, max(openCostInBytes, bytesPerCore))

    print(f"totalBytes:{totalBytes},bytesPerCore:{bytesPerCore},maxSplitBytes:{maxSplitBytes}")

    files_split = []
    for file in files:
        offsets = list(range(0, file, maxSplitBytes))
        file_split = []
        for offset in offsets:
            remaining = file - offset
            size = 0
            if remaining > maxSplitBytes:
                size = maxSplitBytes
            else:
                size = remaining
            file_split.append((offset, size))
        files_split.append(file_split)
    return files_split


if __name__=="__main__":
    parquet_4 = [180633692,
                 180592610,
                 180931551,
                 180936221]

    splits=get_split(parquet_4)

    for split in splits:
        print(split)
        print(len(split))