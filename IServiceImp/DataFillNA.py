#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : DataFillNA.py
@ Author: LengWei
@ Date  : 2019-11-26
@ Desc  : 数据缺失值填充，完成类别列、数值列的空值处理

"""
import pandas as pd

from IService.DService import DService


class DataFillNA(DService):

    def __init__(self):
        pass

    def __fill_value(self, col, method):
        """
        分支判断进行空值填充
        :param col:
        :param method:
        :return:
        """
        # 数据探索表填充
        if method in self.prefileDf.columns:
            v = self.prefileDf[self.prefileDf['Columns'] == col][method]
            fill_value = v.values[0]
            self.df[col].fillna(fill_value, inplace=True)
        # 常数值填充
        elif 'CONST' in method:
            # 数值类型填充常数值
            if 'int' in str(self.df[col].dtype):
                fill_value = int(method.split('_')[1])
                self.df[col].fillna(fill_value, inplace=True)
            elif 'float' in str(self.df[col].dtype):
                fill_value = float(method.split('_')[1])
                self.df[col].fillna(fill_value, inplace=True)
            # 字符串（类别）类型填充常数值
            else:
                fill_value = method.split('_')[1]
                self.df[col].fillna(fill_value, inplace=True)
        # 日期类型填充
        elif 'bfill' == method or 'ffill' == method:
            # 只对 datetime 类型做 ffill/bfill
            if 'datetime' in str(self.df[col].dtype):
                self.df[col].fillna(method=method, inplace=True)
        return self.df

    def __fill_na(self, defaultFill, colsMap):
        """
        根据控制参数，实现对缺失值的填充
        :param df:
        :param prefileDf:
        :param fill_methods:
        :return:
        """
        is_null = self.df.isnull().any()
        type_series = self.df.dtypes
        type_dict = type_series.to_dict()
        method_dict = dict()
        for col, _type in type_dict.items():
            if not is_null[col]:
                continue
            # 列映射填充方法
            if col in colsMap:
                self.df = self.__fill_value(col, colsMap[col])
                method_dict[col] = colsMap[col]
            # 依据字段类型进行大范围填空值
            elif 'int' in str(_type):
                self.df = self.__fill_value(col, defaultFill['num'])
            elif 'float' in str(_type):
                self.df = self.__fill_value(col, defaultFill['num'])
            elif 'object' in str(_type):
                self.df = self.__fill_value(col, defaultFill['enum'])
            elif 'category' in str(_type):
                self.df = self.__fill_value(col, defaultFill['enum'])
            elif 'datetime' in str(_type):
                self.df = self.__fill_value(col, defaultFill['datetime'])
            # 其他类型，按字符串处理
            else:
                self.df = self.__fill_value(col, defaultFill['enum'])
        return self.df

    def process(self, dicInput, dicParams):
        """
        :*功能*: 对数据帧进行不同数据列的空值填充. (2 ==> 2)
        ：*约束*： 输入的数据帧的列已经过筛选，全部为时间列

        :param dicInput:  <dict> 输入的DataFrame数据表。形式:{'inputDf1':<dataDf>, 'inputDf2':<profileDf>}
        :param dicParams:  （类似yaml格式）
                - mapping : <dict> 必选。
                    { 列名： 填充方法名 } 的字典，填充值支持函数形式。
                - 其他参数。

        :*示例*:
            ::
              mapping: {"MONTH_ON_BOOK":-1, "EDUCA":4, "OCC_CATGRY":5, "YR_IN_COMP":0}  # support functions
              outfile: "BaseFeatureDerive_CP08"

        :return: dicOutput :  <dict> 输出的DataFrame数据表。形式: {'outputDf1': <dataDf>}
        """
        dicOutput = dict()

        # 输入数据
        inputDf = dicInput['inputDf1']
        prefileDf = dicInput['inputDf2']

        # 控制参数
        mapping = dicParams['mapping']
        defaultFill = mapping['defaultFill']
        colsMap = mapping['colsMap']

        # 数据处理
        self.df = inputDf
        self.prefileDf = prefileDf
        outputDf = self.__fill_na(defaultFill, colsMap)

        # 输出
        dicOutput['outputDf'] = outputDf

        return dicOutput
