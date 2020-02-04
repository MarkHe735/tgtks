#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd


class RowCleaningService(DService):
    """
    功能        清洗低质量数据行

    输入        待清洗的数据        　数据帧类型     data
    　
    控制      　缺失字段比率阀值      浮点类型     　thresholdOfMissingRatio              0.8
    　
    输出        清洗后的数据      　　数据帧类型     dataCleaned
    """

    def process(self, dicInput, dicParams):

        data = dicInput.get('data')
        if isinstance(data, type(None)):
            raise Exception("no input data")

        thresholdOfMissingRatio = dicParams.get('thresholdOfMissingRatio')
        if thresholdOfMissingRatio is None:
            thresholdOfMissingRatio = 0.8

        if (data.shape[1] == 0):
            dataCleaned = pd.DataFrame()
        else:
            dataCleaned = self.__dataRowCleaning(data, thresholdOfMissingRatio)

        dicOutput = dict()
        dicOutput['dataCleaned'] = dataCleaned

        return dicOutput

    def __dataRowCleaning(self, data, thresholdOfMissingRatio):
        """
        清洗低质量数据行
        :param data: 待清洗的数据
        :param thresholdOfMissingRatio: 缺失字段比率阀值
        :return: 清洗后的数据
        """
        # 记录的长度（列数）
        length = data.shape[1]

        # 计算每条记录的缺失值数量
        data['_number_missing'] = data.isnull().sum(axis=1)
        # 计算每条记录是否小于阈值的标记
        flag = (data['_number_missing'] / length) < thresholdOfMissingRatio

        data2 = data[flag]
        # 删除用于计算的字段
        data2.pop('_number_missing')

        return data2
