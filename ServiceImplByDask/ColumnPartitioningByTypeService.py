#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService


class ColumnPartitioningByTypeService(DService):
    """
           功能        按照列类型拆分数据

           输入        待分割数据     数据帧类型      data

           控制     　 选择列          字符串列表     colsIncluded

           输出        数值列分区      数据帧类型      partitionOfNumberColumns
           输出        类别列分区      数据帧类型      partitionOfCategoryColumns
           输出        时间列分区      数据帧类型      partitionOfDatetimeColumns
           输出        其他列分区      数据帧类型      partitionOfOtherColumns

       """

    def process(self, dicInput, dicParams):

        data = dicInput.get('data')
        if isinstance(data, type(None)):
            raise Exception("no input data")

        colsIncluded = dicParams.get('colsIncluded')
        if colsIncluded is None:
            colsIncluded = ['label', 'id']

        partitionOfNumberColumns, partitionOfCategoryColumns, partitionOfDatetimeColumns, partitionOfOtherColumns = \
            self.__dataColumnPartitioning(data, colsIncluded)

        dicOutput = dict()
        dicOutput['partitionOfNumberColumns'] = partitionOfNumberColumns
        dicOutput['partitionOfCategoryColumns'] = partitionOfCategoryColumns
        dicOutput['partitionOfDatetimeColumns'] = partitionOfDatetimeColumns
        dicOutput['partitionOfOtherColumns'] = partitionOfOtherColumns

        return dicOutput

    def __dataColumnPartitioning(self, data, colsIncluded):
        """
        按照列类型拆分数据
        :param data: 待分割数据
        :param colsIncluded: 选择列，应单独分组的列（partitionOfOtherColumns）
        :return: 数值列分区、类别列分区、时间列分区、其他列分区
        """
        numberColumnList = []
        categoryColumnList = []
        datetimeColumnList = []
        otherColumnList = []

        types = data.dtypes
        indexs = types.index
        length = len(indexs)

        # 循环各列
        for i in range(length):
            colName = indexs[i]
            colType = str(types[colName])
            # 判断各列类型
            if colName in colsIncluded:
                otherColumnList.append(colName)
            elif 'int' in colType or 'float' in colType:
                numberColumnList.append(colName)
            elif 'datetime' in colType:
                datetimeColumnList.append(colName)
            elif 'object' == colType:
                categoryColumnList.append(colName)

        # 筛选各个分组列，返回的是dataframe
        partitionOfNumberColumns = data[numberColumnList].compute()
        partitionOfCategoryColumns = data[categoryColumnList].compute()
        partitionOfDatetimeColumns = data[datetimeColumnList].compute()
        partitionOfOtherColumns = data[otherColumnList].compute()

        return partitionOfNumberColumns, partitionOfCategoryColumns, partitionOfDatetimeColumns, partitionOfOtherColumns
