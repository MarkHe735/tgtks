#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : DataColsTypeSplit.py
@ Author: LengWei
@ Date  : 2019-11-26
@ Desc  : 数据帧根据列类型进行拆分，输出不同类别数据帧

"""

import dask.dataframe as pd
import dask
from datetime import datetime
from IService.DService import DService

'''
idCol = 'id'
labelCol = 'label'
dateparse = lambda dates: datetime.strptime(dates, '%Y-%m-%d')
RawData = pd.read_csv('/home/wj/code3/knimeModule/knime20191126/data/RawData.csv', parse_dates=['applyDate'],
                      date_parser=dateparse,
                      dtype={'id': int, 'col1': object, 'col2': object, 'col3': object, 'value1': int, 'value2': int,
                             'label': int})
'''


class DataColsTypeSplit(DService):

    def __init__(self):
        pass

    def DataSplit(self, RawData, idCol, labelCol):
        """
        一个数据表拆分为4个数据表:
            1、只包含时间格式的数据集（含id列）
            2、只包含数值格式的数据集（含id列）
            3、只包含类别格式的数据集（含id列）
            4、只包含label的数据（含id列）
        其中：1-3可以有空数据集，但不能同时为空，4必须有数据
        :param RawData: 原始数据集，需要提前标注各列的数据格式
        :param idCol: id列
        :param labelCol: label标签列
        :return: 拆分后的数据集
        """
        # 提取时间格式数据（含id）
        DateDf = RawData.select_dtypes(include=['datetime', 'timedelta', 'datetimetz'], exclude=None)
        if idCol not in DateDf.columns.tolist():
            DateDf = pd.concat([DateDf, RawData[idCol]], axis=1)
        if labelCol in DateDf.columns.tolist():
            DateDf = DateDf.drop(columns=labelCol)

        # 提取数值类型数据（含id）
        ValueDf = RawData.select_dtypes(include=['number'], exclude=None)
        if idCol not in ValueDf.columns.tolist():
            ValueDf = pd.concat([ValueDf, RawData[idCol]], axis=1)
        if labelCol in ValueDf.columns.tolist():
            ValueDf = ValueDf.drop(columns=labelCol)

        # 提取类别类型数据（含id）
        ClassDf = RawData.select_dtypes(include=[object, 'category', bool], exclude=None)
        if idCol not in ClassDf.columns.tolist():
            ClassDf = pd.concat([ClassDf, RawData[idCol]], axis=1)
        if labelCol in ClassDf.columns.tolist():
            ClassDf = ClassDf.drop(columns=labelCol)

        # 提取label（含id）
        LabelDf = RawData[[idCol, labelCol]]

        return dask.compute(DateDf, ValueDf, ClassDf, LabelDf)

    def process(self, dicInput, dicParams):
        """
        :*功能*: 对数据帧根据时间类型、类别类型、数值类型，标签类型进行拆分，输出多个纯类型数据集（包含ID列）.

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

        :return: dicOutput :  <dict> 输出的DataFrame数据表。形式: {'outputDf1': <dateDf>, 'outputDf2':<numericDf>，
                                'outputDf3': <categoryDf>, 'outputDf4':<labelDf>}}
        """
        dicOutput = dict()

        # 输入数据
        RawData = dicInput.get('inputDf')

        # 控制参数
        mapping = dicParams['mapping']
        idCol = mapping.get('idCol')
        labelCol = mapping.get('labelCol')

        # 数据处理
        DateDf, ValueDf, ClassDf, LabelDf = self.DataSplit(RawData, idCol, labelCol)

        # 输出
        # outputDf = inputDf
        dicOutput['outputDf1'] = DateDf
        dicOutput['outputDf2'] = ValueDf
        dicOutput['outputDf3'] = ClassDf
        dicOutput['outputDf4'] = LabelDf

        return dicOutput
