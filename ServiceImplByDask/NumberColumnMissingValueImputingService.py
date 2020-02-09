#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd


class NumberColumnMissingValueImputingService(DService):
    """
     功能     填充数值列缺失值

    　输入    待处理的数据     数据帧类型     dataOfNumberColumn       要求输入data全部为类别列
      输入    数据列数据画像   数据帧类型     dataProfile

    　控制    缺省处理方法  　 字符串类型     methodDefault            mean            mean,median,const_value
      控制    处理方法字典     字典类型       dictOfMethod             {}
    　
    　输出    无缺失值的数据   数据帧类型     dataWithoutMissingValue

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
            methodDefault = 'mean'

        dictOfMethod = dicParams.get('dictOfMethod')
        if dictOfMethod is None:
            dictOfMethod = dict()

        if (data.shape[1] == 0):
            dataWithoutMissingValue = pd.DataFrame()
        else:
            dataWithoutMissingValue = self.__numberColumnMissingValueImputing(data, dataProfile, methodDefault,
                                                                              dictOfMethod)

        dicOutput = dict()
        dicOutput['dataWithoutMissingValue'] = dataWithoutMissingValue

        return dicOutput

    def __numberColumnMissingValueImputing(self, data, dataProfile, methodDefault, dictOfMethod):
        """
        填充数值列缺失值
        :param data: 待处理的数据
        :param dataProfile: 数据列数据画像
        :param methodDefault: 缺省处理方法
        :param dictOfMethod: 处理方法字典
        :return: 无缺失值的数据
        """
        dataProfile = dataProfile.set_index(dataProfile['col_name'])
        # TODO: 考虑deep copy的内存占用问题
        # data_copy = data.copy(deep=True)
        # 循环各列
        for colName in data.columns:
            method = dictOfMethod.get(colName)
            if method is None:
                method = methodDefault
            # 获取填充值
            if method == 'mean':
                value = dataProfile.loc[colName, 'mean']
            elif method == 'median':
                value = dataProfile.loc[colName, '50%']
            elif method.startswith('const_'):
                # 空值填充为指定的常数值
                value = float(method[6:])
            else:
                raise Exception("error method")
            data[colName] = data[colName].fillna(value)
        data = data.compute()

        return data
