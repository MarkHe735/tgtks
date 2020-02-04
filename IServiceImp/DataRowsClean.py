#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : DataRowsClean.py
@ Author: LengWei
@ Date  : 2019-11-26
@ Desc  : 数据行缺失处理，过滤掉缺失严重的列

"""
from IService.DService import DService


class DataRowsClean(DService):

    def __init__(self):
        pass

    def __row_clean(self, inputDf, threshold):
        """
        根据阈值删除行
        :param inputDf:
        :param threshold:
        :return:
        """
        thres_row = inputDf.isnull().sum(axis=1) / len(inputDf.columns) < threshold
        outputDf = inputDf[thres_row]
        # # calculate number of na in a row
        # thres = round(len(inputDf.columns) * (1 - threshold) + 1)
        # outputDf = inputDf.dropna(axis=0, thresh=thres)
        return outputDf

    def process(self, dicInput, dicParams):
        """
        :*功能*: 对数据帧进行缺失行筛选.

        :param dicInput:  <dict> 输入的DataFrame数据表。形式:{'inputDf':<DataFrame>}
        :param dicParams:  （类似yaml格式）
                - mapping : <dict> 必选。
                    { 列名：填充值} 的字典，填充值支持函数形式。
                - missingThreshold: float ， default 0.9。

        :*示例*:
            ::
               CP08:
                  mapping: {"MONTH_ON_BOOK":-1, "EDUCA":4, "OCC_CATGRY":5, "YR_IN_COMP":0}  # support functions
                  outfile: "BaseFeatureDerive_CP08"

        :return: dicOutput :  <dict> 输出的DataFrame数据表。形式: {'outputDf1': <dataDf>, 'outputDf2':<filtedDf>}
        """
        dicOutput = dict()

        # 输入数据
        inputDf = dicInput['inputDf']

        # 控制参数
        mapping = dicParams['mapping']
        miss_rate = mapping['MissRate']

        # 数据处理
        outputDf = self.__row_clean(inputDf, miss_rate)

        # 输出
        dicOutput['outputDf'] = outputDf

        return dicOutput
