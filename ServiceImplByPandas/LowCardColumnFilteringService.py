#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd


class LowCardColumnFilteringService(DService):
    """
     功能     过滤低基数数值列（自动识别数字编码）

    　输入    待转换的数据     　    数据帧类型      data                  全部为数值列
      输入    数据列数据画像    　   数据帧类型      dataProfile
    　
    　控制    基数阀值               整数类型       thresholdOfCard        10
    　
    　输出    转换后的类别列数据     数据帧类型     dataOfLowCardColumn
      输出    转换后的数值列数据     数据帧类型     dataOfHighCardColumn

    """

    def process(self, dicInput, dicParams):

        data = dicInput.get('data')
        if isinstance(data, type(None)):
            raise Exception("no input data")

        dataProfile = dicInput.get('dataProfile')
        if isinstance(dataProfile, type(None)):
            raise Exception("no input dataProfile")

        thresholdOfCard = dicParams.get('thresholdOfCard')
        if thresholdOfCard is None:
            thresholdOfCard = 15

        if (data.shape[1] == 0):
            dataOfLowCardColumn = pd.DataFrame()
            dataOfHighCardColumn = pd.DataFrame()
        else:
            dataOfLowCardColumn, dataOfHighCardColumn = self.__lowCradNumberColumnFiltering(data, dataProfile,
                                                                                            thresholdOfCard)

        dicOutput = dict()
        dicOutput['dataOfLowCardColumn'] = dataOfLowCardColumn
        dicOutput['dataOfHighCardColumn'] = dataOfHighCardColumn

        return dicOutput

    def __lowCradNumberColumnFiltering(self, data, dataProfile, thresholdOfCard):
        """
        过滤低基数数值列（自动识别数字编码）
        :param data: 待转换的数据
        :param dataProfile: 数据列数据画像
        :param thresholdOfCard: 基数阀值
        :return: 转换后的类别列数据: dataOfLowCardColumn, 转换后的数值列数据: dataOfHighCardColumn
        """
        dataProfile = dataProfile.set_index(dataProfile['col_name'])

        lowList = []
        HighList = []
        # 循环数据各列
        for colName in dataProfile.index:
            card = dataProfile.loc[colName, 'card']
            # 小于阈值的列分类为低基数列
            if card < thresholdOfCard:
                lowList.append(colName)
            else:
                HighList.append(colName)
        # 筛选分类的列
        dataOfLowCardColumn = data[lowList]
        dataOfHighCardColumn = data[HighList]
        return dataOfLowCardColumn, dataOfHighCardColumn
