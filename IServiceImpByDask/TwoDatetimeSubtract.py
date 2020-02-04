#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : TwoDatetimeSubtract.py
@ Author: WangJun
@ Date  : 2019-12-05
@ Desc  : 两列时间求差：指定时间列做减法运算，精确到秒

"""
import pandas as pd
from IService.DService import DService


class TwoDatetimeSubtract(DService):

    def __init__(self):
        pass

    def __datetime_subtract(self, df, datetime_dict):
        """
        对指定时间列做减法运算，精确到秒
        :param df:
        :param datetime_dict:
        :return:
        """
        time_format = "%Y%m%d %H:%M:%S"
        for date1, date2 in datetime_dict.items():
            df[date1 + '-' + date2] = (pd.to_datetime(df[date1], format=time_format) -
                                       pd.to_datetime(df[date2], format=time_format)).dt.total_seconds()
            df = df.drop([date1, date2], axis=1)
        return df

    def process(self, dicInput, dicParams):
        """
        :*功能*: 对数据帧中的类别列进行编码衍生，生成离散类别列. (1 ==> 1)

        :param dicInput:  <dict> 输入的DataFrame数据表。形式:{'inputDf':<dataDf>}
        :param dicParams:  （类似yaml格式）
                - mapping : <dict> 必选。
                    { 列名：分箱方法名 } 的字典，填充值支持函数形式。
                -

        :*示例*:
            ::
               CP08:
                  mapping: {"MONTH_ON_BOOK":-1, "EDUCA":4, "OCC_CATGRY":5, "YR_IN_COMP":0}  # support functions
                  outfile: "BaseFeatureDerive_CP08"

        :return: dicOutput :  <dict> 输出的DataFrame数据表。形式: {'outputDf1': <dataDf>, 'outputDf2': <transDf>}
        """
        dicOutput = dict()

        # 输入数据
        inputDf = dicInput['inputDf']

        # 控制参数
        mapping = dicParams['mapping']

        # 数据处理
        outputDf = self.__datetime_subtract(inputDf, mapping)

        # 输出
        dicOutput['outputDf'] = outputDf

        return dicOutput
