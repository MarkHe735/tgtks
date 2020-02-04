#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService


class ColumnFilteringService(DService):
    """
    功能        过滤列

     输入       待过滤的数据     　数据帧类型      data
    　
     控制     　选择列           　字符串列表     colsIncluded
    　
    输出        清洗后的数据       数据帧类型     dataWithColumnIncluded
    输出        清洗后的数据       数据帧类型     dataWithoutColumnIncluded

    """

    def process(self, dicInput, dicParams):

        data = dicInput.get('data')
        if isinstance(data, type(None)):
            raise Exception("no input data")
        if (data.shape[1] == 0):
            raise Exception("data col_number = 0")

        colsIncluded = dicParams.get('colsIncluded')
        if colsIncluded is None:
            colsIncluded = []

        dataWithColumnIncluded, dataWithoutColumnIncluded = self.__columnFiltering(colsIncluded, data)

        dicOutput = dict()
        dicOutput['dataWithColumnIncluded'] = dataWithColumnIncluded
        dicOutput['dataWithoutColumnIncluded'] = dataWithoutColumnIncluded

        return dicOutput

    def __columnFiltering(self, colsIncluded, data):
        """
        过滤列，保留指定的列
        :param colsIncluded: 待过滤的数据
        :param data: 选择列
        :return: 清洗后的数据
        """
        colsExcluded = []
        # 过滤舍弃的各个字段
        for colName in data.columns:
            if colName in colsIncluded:
                pass
            else:
                colsExcluded.append(colName)
        # 将数据分组
        dataWithColumnIncluded = data[colsIncluded]
        dataWithoutColumnIncluded = data[colsExcluded]

        return dataWithColumnIncluded, dataWithoutColumnIncluded
