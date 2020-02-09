#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd


class NumberColumnOutlierProcessingService(DService):
    """
     功能        处理数值列异常值

    　输入        待处理的数据     数据帧类型     dataOfNumberColumn     要求输入data全部为数值列
      输入        数据列数据画像   数据帧类型     dataProfile

    　控制        缺省处理方法   　字符串类型     methodDefault          iqr      iqr,sigma
      控制        处理方法字典     字典类型       dictOfMethod           {}
    　
    　输出        无缺失值的数据   数据帧类型     dataWithoutOutlier

    """

    def process(self, dicInput, dicParams):

        data = dicInput.get('dataOfNumberColumn')
        if isinstance(data, type(None)):
            raise Exception("no input dataOfNumberColumn")

        dataProfile = dicInput.get('dataProfile')
        if isinstance(dataProfile, type(None)):
            raise Exception("no input dataProfile")

        methodDefault = dicParams.get('methodDefault')
        if methodDefault is None:
            methodDefault = 'iqr'

        dictOfMethod = dicParams.get('dictOfMethod')
        if dictOfMethod is None:
            dictOfMethod = dict()

        if (data.shape[1] == 0):
            dataWithoutOutlier = pd.DataFrame()
        else:
            dataWithoutOutlier = self.__numberColumnOutlierProcessing(data, dataProfile, dictOfMethod, methodDefault)

        dicOutput = dict()
        dicOutput['dataWithoutOutlier'] = dataWithoutOutlier

        return dicOutput

    def __numberColumnOutlierProcessing(self, data, dataProfile, dictOfMethod, methodDefault):
        """
        处理数值列异常值
        :param data: 待处理的数据
        :param dataProfile: 数据列数据画像
        :param dictOfMethod: 缺省处理方法
        :param methodDefault: 处理方法字典
        :return: 无缺失值的数据
        """
        dataProfile = dataProfile.set_index(dataProfile['col_name'])
        # TODO: 考虑deep copy的内存占用问题
        # data_copy = data.copy(deep=True)
        # 循环各列
        data = data.compute()
        for colName in data.columns:
            # 获取异常值处理方法
            method = dictOfMethod.get(colName)
            if method is None:
                method = methodDefault
            # 获取数值边界
            # value_max = dataProfile.loc[colName, 'max']
            # value_min = dataProfile.loc[colName, 'min']
            if method == 'iqr':
                iqr = dataProfile.loc[colName, '75%'] - dataProfile.loc[colName, '25%']
                value_max = dataProfile.loc[colName, '75%'] + 2.5 * iqr
                value_min = dataProfile.loc[colName, '25%'] - 2.5 * iqr
            elif method == 'sigma':
                std = dataProfile.loc[colName, 'std']
                value_max = dataProfile.loc[colName, 'mean'] + 4.0 * std
                value_min = dataProfile.loc[colName, 'mean'] - 4.0 * std
            else:
                raise Exception("error method")
            # 用边界值替换异常值
            index = data[colName] > value_max
            data.loc[index, colName] = value_max
            index = data[colName] < value_min
            data.loc[index, colName] = value_min

        return data
