#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : DatetimeColsDeriv.py
@ Author: WangJun
@ Date  : 2019-11-26
@ Desc  : 对时间列进行衍生，生成衍生类别列

"""
import pandas as pd
from datetime import datetime
# from chinese_calendar import is_workday, is_holiday
import dask.dataframe as dd
import math

from IService.DService import DService

'''
idCol = 'id'
labelCol = 'label'
dateparse = lambda dates: datetime.strptime(dates, '%Y/%m/%d')
DateDf = pd.read_csv('/home/wj/code3/knimeModule/knime20191126/data/TrainData_datatime.csv', parse_dates=['applyDate'],
                      date_parser=dateparse)

df['is_holiday'] = df['统计日期'].apply(lambda x: is_workday(x))
'''


class DatetimeColsDeriv(DService):

    def __init__(self):
        pass

    def DateDeriv(self, DateDf, idCol):
        """
        :param DateDf: 只包含日期格式和id列的数据集
        :param idCol: id列
        :return: 衍生之后的数据集
        """
        ColsAll = DateDf.columns.tolist()
        ColsAll_filter = list(set(ColsAll) - {idCol})
        for col in ColsAll_filter:
            DateDf[col + '_yearSeq'] = dd.Series([x.year for x in DateDf[col]])
            DateDf.loc[:, col + '_monthSeq'] = [x.month for x in DateDf.loc[:, col]]
            DateDf.loc[:, col + '_monthDay'] = [x.day for x in DateDf.loc[:, col]]
            DateDf.loc[:, col + '_weekSeq'] = [x.isocalendar()[1] for x in DateDf.loc[:, col]]

            DateDf.loc[:, col + '_weekday'] = [x.weekday() for x in DateDf.loc[:, col]]  # weekday是0-6，需要转换成1-7
            DateDf.loc[:, col + '_weekday'] = [x + 1 for x in DateDf.loc[:, col + '_weekday']]

            DateDf.loc[:, col + '_quarter'] = [int(math.floor((x + 2) / 3)) for x in DateDf.loc[:, col + '_monthSeq']]

            DateDf.loc[:, col + '_halfMonth'] = DateDf.loc[:, col + '_monthDay'].apply(lambda x: 1 if x <= 15 else 2)
            DateDf.loc[:, col + '_halfYear'] = DateDf.loc[:, col + '_monthSeq'].apply(lambda x: 1 if x <= 6 else 2)

            DateDf = DateDf.reset_index(drop=True)

            for i in range(len(DateDf)):
                if DateDf.loc[i, col + '_monthDay'] <= 10:
                    DateDf.loc[i, col + '_xun'] = 1
                elif DateDf.loc[i, col + '_monthDay'] > 20:
                    DateDf.loc[i, col + '_xun'] = 3
                else:
                    DateDf.loc[i, col + '_xun'] = 2
            '''
            for i in range(len(DateDf)):
                if (DateDf.loc[i, col + '_monthDay'] <= 15) & (DateDf.loc[i, col + '_workday'] == 1):
                    DateDf.loc[i, col + '_weekendHalfMonth'] = 1
                elif (DateDf.loc[i, col + '_monthDay'] > 15) & (DateDf.loc[i, col + '_workday'] == 1):
                    DateDf.loc[i, col + '_weekendHalfMonth'] = 2
                else:
                    DateDf.loc[i, col + '_weekendHalfMonth'] = 0

            for i in range(len(DateDf)):
                if (DateDf.loc[i, col + '_monthDay'] <= 10) & (DateDf.loc[i, col + '_workday'] == 1):
                    DateDf.loc[i, col + '_weekendXun'] = 1
                elif (DateDf.loc[i, col + '_monthDay'] > 10) & (DateDf.loc[i, col + '_monthDay'] <= 20) & (
                        DateDf.loc[i, col + '_workday'] == 1):
                    DateDf.loc[i, col + '_weekendXun'] = 2
                elif (DateDf.loc[i, col + '_monthDay'] > 20) & (DateDf.loc[i, col + '_workday'] == 1):
                    DateDf.loc[i, col + '_weekendXun'] = 3
                else:
                    DateDf.loc[i, col + '_weekendXun'] = 0

            '''
        ColsAllDeriv = DateDf.columns.tolist()
        ColsAllDerivKeep = list(set(ColsAllDeriv) - set(ColsAll_filter))
        DateDerivDf = DateDf[ColsAllDerivKeep]
        return DateDerivDf.compute()

    def process(self, dicInput, dicParams):
        """
        :*功能*: 对数据帧中的时间列进行衍生，生成年、月、日等类别列. (1 ==> 1)

        :param dicInput:  <dict> 输入的DataFrame数据表。形式:{'inputDf':<dataDf>}
        :param dicParams:  （类似yaml格式）
                - mapping : <dict> 必选。
                    { 列名：填充值} 的字典，填充值支持函数形式。
                - operate : 'keep'/'drop',  default 'keep'。

        :*示例*:
            ::
               CP08:
                  mapping: {"MONTH_ON_BOOK":-1, "EDUCA":4, "OCC_CATGRY":5, "YR_IN_COMP":0}  # support functions
                  outfile: "BaseFeatureDerive_CP08"

        :return: dicOutput :  <dict> 输出的DataFrame数据表。形式: {'outputDf1': <dataDf>}
        """
        dicOutput = dict()

        # 输入数据
        DateDf = dicInput.get('inputDf')

        # 控制参数
        mapping = dicParams['mapping']
        idCol = mapping.get('idCol')

        # 数据处理
        DateDerivDf = self.DateDeriv(DateDf, idCol)
        # 输出
        # outputDf = inputDf
        dicOutput['outputDf1'] = DateDerivDf

        return dicOutput
