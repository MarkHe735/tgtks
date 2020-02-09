#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import numpy as np
import pandas as pd


class ToCategoryConvertingService(DService):
    """
     功能        转换全部列为类别列

    　输入       待转换的数据     　数据帧类型      data

      输出       转换后的数据       数据帧类型      dataOfCateorgyColumn

    """

    def process(self, dicInput, dicParams):

        data = dicInput.get('data')
        if isinstance(data, type(None)):
            raise Exception("no input data")

        if (data.shape[1] == 0):
            des = pd.DataFrame()
        else:
            des = self.__toCategoryConverting(data)

        dicOutput = dict()
        dicOutput['dataOfCateorgyColumn'] = des
        return dicOutput

    def __toCategoryConverting(self, data):
        """
        转换全部列为类别列
        :param data: 待转换的数据
        :return: 转换后的数据
        """
        # 转换类型
        d = data.astype('str')
        # 循环各列
        for colName in d.columns:
            # 将所有值均加后缀：S
            d[colName] = d[colName].map(lambda x: str(x) + 'S')
        # 更正nan值
        d = d.replace('nanS', np.NaN)
        d = d.compute()
        return d
