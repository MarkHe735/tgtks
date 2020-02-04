#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : DataColsClean.py
@ Author: LengWei
@ Date  : 2019-11-26
@ Desc  : 数据清洗，完成去除常数列、ID列、高空值率的列

"""
from IService.DService import DService


class DataColsClean(DService):

    def __init__(self):
        pass

    def __delete_columns(self, inputDf, prefileDf, retain_col, missThresholdDict, missThreshold, uniqueThreshold,
                         constantThreshold):
        """
        根据阈值删除列，需保留参数中规定要保留的列
        :param inputDf:
        :param prefileDf:
        :param retain_col:
        :param missThreshold:
        :param missThresholdDict:
        :return:
        """
        for _, row in prefileDf.iterrows():
            col = row['Columns']
            miss_rate = row['MissRate']
            unique_rate = row['UniqueRate']
            constant_rate = row['ConstantRate']
            # get threshold
            if col in retain_col:
                continue
            elif col in missThresholdDict:
                miss_threshold = missThresholdDict[col]
            else:
                miss_threshold = missThreshold
            unique_threshold = uniqueThreshold
            constant_threshold = constantThreshold
            # delete columns that beyond thresholds.
            if miss_rate >= miss_threshold:
                inputDf.drop(columns=[col], axis=1, inplace=True)
            elif unique_rate >= unique_threshold:
                inputDf.drop(columns=[col], axis=1, inplace=True)
            elif constant_rate >= constant_threshold:
                inputDf.drop(columns=[col], axis=1, inplace=True)

        return inputDf

    def process(self, dicInput, dicParams):
        """
        :*功能*: 对数据帧进行数据清洗.(1 ==> 1)

        :param dicInput:  <dict> 输入的DataFrame数据表。形式:{'inputDf1':<DataFrame>, 'inputDf2':<profileDf}
        :param dicParams:  （类似yaml格式）
                - mapping : <dict> 必选。
                    { 列名：填充值} 的字典，填充值支持函数形式。
                -

        :*示例*:
            ::
              mapping: {"MONTH_ON_BOOK":-1, "EDUCA":4, "OCC_CATGRY":5, "YR_IN_COMP":0}  # support functions
              outfile: "BaseFeatureDerive_CP08"

        :return: dicOutput :  <dict> 输出的DataFrame数据表。形式: {'outputDf': <colfiltedDf>}
        """
        dicOutput = dict()

        # 输入数据
        inputDf = dicInput['inputDf1']
        prefileDf = dicInput['inputDf2']

        # 控制参数
        mapping = dicParams['mapping']
        retain_col = mapping['keepCols']
        missThreshold = mapping['missThreshold']
        uniqueThreshold = mapping['uniqueThreshold']
        constantThreshold = mapping['constantThreshold']
        missThresholdDict = mapping['missThresholdDict']

        # 数据处理
        outputDf = self.__delete_columns(inputDf, prefileDf, retain_col, missThresholdDict, missThreshold, uniqueThreshold,
                                         constantThreshold)

        # 输出
        dicOutput['outputDf'] = outputDf

        return dicOutput
