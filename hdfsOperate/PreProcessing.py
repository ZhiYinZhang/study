#!/usr/bin/env python
# -*- coding:utf-8 -*-
# datetime:2018/10/16 14:01

import os
import sys
import re
import time
import matplotlib.pyplot as plt
from pandas import read_csv
from hdfs.client import Client


class DataPreProcessor(object):
    _separation = ','
    _column_num = None
    _row_num = None
    _is_hdfs = False
    data = None
    _file_size = None
    properties = {}
    _property_limit = {"count", "dtypes", "max", "min", "median", "var", "std"}

    def __getitem__(self, item):
        if item not in self._property_limit:
            raise RuntimeError("Don't support %s property" % item)
        pro_res = self.properties.get(item, None)
        if not pro_res:
            self._set_property(item)
        return self.properties[item]

    @staticmethod
    def _is_hdfs_path(file_path):
        """
        判断是否为hdfs路径，支持8020与9000端口
        support 8082 and 9000 port
        :param file_path:
        :return: Bool
        """
        if re.search(r"hdfs://.*:(?:8020|9000)", file_path) is not None:
            return True
        return False

    @staticmethod
    def _parse_hdfs_path(file_path):
        """
        解析标准hdfs路径，转化为hdfs库所需路径
        parse a normal hdfs path
        support 8082 and 9000 port
        :return: host ip and file location
        """
        s_res = re.search(r"hdfs://(.*):(?:8020|9000)(/.*)", file_path)
        host = "http://" + s_res.group(1) + ":50070"
        path = s_res.group(2)
        return host, path

    def _set_property(self, item):
        """
        设置对应属性
        :param item: 属性名
        """
        if item not in self._property_limit:
            raise RuntimeError("Don't support %s property" % item)
        if item == "dtypes":
            dtypes = self.data.dtypes.to_dict()
            for ty in dtypes:
                dtypes[ty] = str(dtypes[ty])
            self.properties[item] = dtypes
        else:
            self.properties[item] = eval("self.data.%s()" % item).to_dict()

    def auto_set_sep(self, file_path):
        """
        取第一行检测分隔符，优先级 'tab' > ',' > ' '
        :param file_path:  路径
        """
        with open(file_path, "r") as f:
            line = f.readline()
        if line.find("\t") >= 0:
            self.set_separation("\t")
            print("Set 'tab' as separation character")
            return
        if line.find(",") >= 0:
            self.set_separation("Set ',' as separation character")
        else:
            self.set_separation("Haven't detect 'tab' or ','. Now set space as separation character")

    def display_columns_properties(self):
        """
        展示所有列的统计属性
        """
        for num in range(self._column_num):
            print("\t\t type of %d column:\t%s" % (num, self.properties[num].type))

    def display_properties(self):
        print("\nFile size is %d bytes" % self._file_size)
        print("Total %s rows and %d columns" % (self._row_num, self._column_num))
        print("\nThe attribute of target file:")
        print("\nThe non empty number of each column:")
        print(self["count"])
        print("\nType of each column:")
        print(self["dtypes"])
        print("unique num:")

    @staticmethod
    def gain_file_size(file_path):
        """
        gain file size. just for local file
        :param file_path: file location
        :return: the size of file(mb).
        """
        file_size = os.path.getsize(file_path)
        return file_size

    def get_column_dist_plot(self, cl_num=0, cl_name=None, bins=10):
        """
        做出一列的直方图
        :param cl_num: 所在列数，如果直接指出列名，该参数将被忽略
        :param cl_name: 列名
        :param bins: 直方图分隔块数
        """
        if cl_name:
            self.plot_column_dist(self.data[cl_name], bins=bins)
            return
        self.plot_column_dist(self.data[cl_num], bins=bins)

    def get_column_value_count(self, cl_num=0, cl_name=None):
        """
        根据列号或者列名，获取某一列的数值分布
        :param cl_num: 所在列数，如果直接指出列名，该参数将被忽略
        :param cl_name: 列名
        :return: Series
        """
        if cl_name:
            return self.data[cl_name].value_counts()
        return self.data[cl_num].value_counts()

    def get_shape(self):
        """
        return the column num and index num of input data
        :return: tuple (row, column)
        """
        return self._row_num, self._column_num

    @staticmethod
    def is_num(col):
        """
        判断某一列的类型是否为数字
        :param col:  判断列名
        :return: Bool
        """
        li_type = {
            "float64",
            "int64",
            "int",
            "float"
        }
        if str(col.dtype) in li_type:
            return True
        return False

    def plot_all_num_columns(self):
        """
        打印出所有数字列的直方图
        """
        for co in self.data:
            if self.is_num(self.data[co]):
                self.plot_column_dist(self.data[co].dropna())

    @staticmethod
    def plot_column_dist(col, bins=10):
        """
        打印一列的直方图
        :param col: 目标列
        :param bins: 直方图分隔块数，默认为10块
        """
        try:
            plt.hist(col, bins=bins)
            plt.title("Column: " + str(col.name))
            plt.show()
        except ValueError as v_error:
            print(v_error)

    def read_file_and_parse(self, file_path, auto_sep=False, header=None):
        """
        读取文件并进行初步解析
        包括：
            获取行列数
            获取每列最大值、最小值
        :param header: csv with header?
        :param file_path: file location
        :param auto_sep:  auto detect the separation of csv file
        """
        if self._is_hdfs_path(file_path):
            self.read_from_hdfs(file_path, header)
        else:
            self.read_from_local(file_path, auto_sep, header)

        # get column num && index num
        (self._row_num, self._column_num) = self.data.shape

    def read_from_local(self, file_path, auto_sep=True, header=None):
        """
        从本地读取文件
        :param file_path: 文件路径
        :param auto_sep: 是否自动获取csv文件的间隔符
        :param header: 文件是否包含header
        """
        is_exist = os.path.exists(file_path)
        self._is_hdfs = False
        if not is_exist:
            print("File path doesn't exist. Please check it up", ResourceWarning)
            exit(1)
        if auto_sep:
            self.auto_set_sep(file_path)
        else:
            if not self._separation:
                print("Seem like you wan't to set separation character manually. "
                      "Please ensure you have set separation value."
                      "Use set_separation to set it.")
                exit(1)

        try:
            self.data = read_csv(file_path, sep=self._separation, header=header)
        # 编码失败，尝试用Latin-1处理
        except UnicodeDecodeError as uni_error:
            print(uni_error)
            print("Detected UnicodeDecodeError. Try other encoding.")
            self.data = read_csv(file_path,
                                 sep=self._separation,
                                 header=header,
                                 encoding="Latin-1")

        self._file_size = self.gain_file_size(file_path)

    def read_from_hdfs(self, file_path, header=None):
        """
        从hdfs读取文件
        :param file_path: 文件路径
        :param header:  文件是否包含header
        """
        host, path = self._parse_hdfs_path(file_path)
        self._is_hdfs = True
        client = Client(host, root="/", timeout=100, session=False)
        self._file_size = client.content(path).get("length")
        try:
            with client.read(path) as reader:
                self.data = read_csv(reader,
                                     sep=self._separation,
                                     header=header)

        except UnicodeDecodeError as uni_error:
            print(uni_error)
            print("Detected UnicodeDecodeError. Try other encoding.")
            with client.read(path, encoding="Latin-1") as reader:
                self.data = read_csv(reader,
                                     sep=self._separation,
                                     header=header,
                                     encoding="Latin-1")

    def set_separation(self, value):
        """
        手动设置分隔符
        set separation character manually
        """
        self._separation = value


if __name__ == '__main__':
    try:
        file_location = sys.argv[1]
    except IndexError as in_error:
        file_location = None
        print("Usage : python PreProcessing.py <file_location>")
        exit(1)
    start_ts = int(time.time())
    _instance = DataPreProcessor()
    _instance.auto_set_sep(file_location)
    # 如果输入文件有header，将header设为0
    _instance.read_file_and_parse(file_location, header=0)
    _instance.display_properties()
    print("\nUsed %d s" % (int(time.time()) - start_ts))
    _instance.plot_all_num_columns()
