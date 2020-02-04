#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd


class DateTimeColumnFeatureGeneratingService(DService):
    """
           功能        时间日期类别特征生成

           输入        待分割数据     数据帧类型      dataOfDateTimeColumn          全部为日期时间列

           输出        其他列分区      数据帧类型     dataOfDateTimeFeature

       """

    def process(self, dicInput, dicParams):

        data = dicInput.get('dataOfDateTimeColumn')
        if isinstance(data, type(None)):
            raise Exception("no input dataOfDateTimeColumn")

        if (data.shape[1] == 0):
            data_copy = pd.DataFrame()
            dicOutput = dict()
            dicOutput['dataOfDateTimeFeature'] = data_copy
            return dicOutput

        # TODO: 应统一做内部方法封装
        data_copy = data.copy(deep=True)
        for colName in data.columns:
            y = data_copy.pop(colName)
            data_copy[colName + '__year'] = y.dt.year
            data_copy[colName + '__month'] = y.dt.month
            data_copy[colName + '__halfyear'] = y.dt.month < 7
            data_copy[colName + '__quar'] = y.dt.month.apply(lambda x: int((x + 2) / 3))
            data_copy[colName + '__halfmonth'] = y.dt.day < 16
            data_copy[colName + '__dayofweek'] = y.dt.dayofweek
            data_copy[colName + '__workday'] = y.dt.dayofweek < 5
            data_copy[colName + '__hour'] = y.dt.hour

        dicOutput = dict()
        dicOutput['dataOfDateTimeFeature'] = data_copy

        return dicOutput
