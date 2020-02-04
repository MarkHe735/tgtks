#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd


class CategoryColumnDataProfilingService(DService):
    """
     功能         对类别列进行画像

    　输入        待质量画像的数据      数据帧类型     dataOfCategoryColumn    要求输入data全部为类别列

    　输出        数据质量画像      　　数据帧类型     dataProfile             众数、基数

    """

    def process(self, dicInput, dicParams):

        data = dicInput.get('dataOfCategoryColumn')
        if isinstance(data, type(None)):
            raise Exception("no input dataOfCategoryColumn")

        if (data.shape[1] == 0):
            des = pd.DataFrame()
        else:
            des = self.__categoryColumnDataProfiling(data)

        dicOutput = dict()
        dicOutput['dataProfile'] = des

        return dicOutput

    def __categoryColumnDataProfiling(self, data):
        """
        对类别列进行画像
        :param data: 待质量画像的数据
        :return: 数据质量画像，众数、基数
        """
        # TODO: 与调用函数时的判断重复，是否需要精简
        if data.shape[1] == 0:
            return pd.DataFrame()

        # 获取以原数据columns为index的dataframe：des
        rowId = data.index[0]
        des = data.head(1).T
        des.pop(rowId)
        # 赋予初始值
        des['card'] = 0
        des['mode'] = ''
        # 逐列计算基数、众数
        for colName in data.columns:
            vcs = data[colName].value_counts()
            des.loc[colName, 'card'] = len(vcs)
            des.loc[colName, 'mode'] = vcs.index[0]
        des['col_name'] = des.index
        return des
