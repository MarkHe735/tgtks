#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
from scipy import stats
import numpy as np
import pandas as pd


class NumberColumnDataProfilingService(DService):
    """
     功能     对数值列进行画像

    　输入    待质量画像的数据    数据帧类型    dataOfNumberColumn    要求输入data全部为数值列
    　
    　输出    数据质量画像      　据帧类型      dataProfile           最大值、最小值、平均数、方差、中位数、上下四分位数、基数

    """

    def process(self, dicInput, dicParams):

        data = dicInput.get('dataOfNumberColumn')
        if isinstance(data, type(None)):
            raise Exception("no input dataOfNumberColumn")

        if (data.shape[1] == 0):
            des = pd.DataFrame()
        else:
            des = self.__numberColumnDataProfiling(data)

        dicOutput = dict()
        dicOutput['dataProfile'] = des
        return dicOutput

    def __numberColumnDataProfiling(self, data):
        """
        对数值列进行画像
        :param data: 待质量画像的数据，要求输入data全部为数值列
        :return: 数据质量画像，最大值、最小值、平均数、方差、中位数、上下四分位数、基数、正太分布相似度
        """
        des = data.describe().compute().T
        des['card'] = 0
        des['mode'] = ''
        # 循环各数值字段
        for colName in des.index:
            dd = data[colName]

            vcs = dd.value_counts().compute()

            # 默认浮点数字段高基数
            type_col = str(dd.dtype)
            # 浮点类型字段基数标记为：100000
            if 'float' in type_col:
                des.loc[colName, 'card'] = 100000
            # 基数为字段不同取值个数
            else:
                des.loc[colName, 'card'] = len(vcs)
            # 各字段众数
            des.loc[colName, 'mode'] = str(vcs.index[0])

            # 正态分布相似度计算（使用显著水平为15%计算）
            res = stats.anderson(dd, 'norm')
            des.loc[colName, 'normal'] = res.statistic < res.critical_values[0]

            des.loc[colName, 'lognormal'] = False
            # 取log后，正态分布相似度计算（使用显著水平为15%计算）
            if des.loc[colName, 'min'] > 0:
                res2 = stats.anderson(np.log(dd), 'norm')
                des.loc[colName, 'lognormal'] = res2.statistic < res2.critical_values[0]

        des.pop('count')
        des['col_name'] = des.index

        return des
