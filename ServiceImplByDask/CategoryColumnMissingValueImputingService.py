#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd
from ServiceImplByPandas.LowCardColumnFilteringService import LowCardColumnFilteringService
from ServiceImplByPandas.NumberColumnDataProfilingService import NumberColumnDataProfilingService
from ServiceImplByPandas.ColumnPartitioningByTypeService import ColumnPartitioningByTypeService
from ServiceImplByPandas.ToCategoryConvertingService import ToCategoryConvertingService


class CategoryColumnMissingValueImputingService(DService):
    """
     功能     填充类别列缺失值

    　输入    待处理的数据     数据帧类型     dataOfCategoryColumn      要求输入data全部为类别列
      输入    数据列数据画像   数据帧类型     dataProfile

    　控制    缺省处理方法  　 字符串类型     methodDefault             mode
      控制    处理方法字典     字典类型       dictOfMethod              {}
    　
    　输出    无缺失值的数据   数据帧类型     dataWithoutMissingValue

    """

    def process(self, dicInput, dicParams):

        data = dicInput.get('dataOfCategoryColumn')
        if isinstance(data, type(None)):
            raise Exception("no input dataOfCategoryColumn")

        dataProfile = dicInput.get('dataProfile')
        if isinstance(dataProfile, type(None)):
            raise Exception("no input dataProfile")

        methodDefault = dicParams.get('methodDefault')
        if methodDefault is None:
            methodDefault = 'mode'

        dictOfMethod = dicParams.get('dictOfMethod')
        if dictOfMethod is None:
            dictOfMethod = dict()

        if (data.shape[1] == 0):
            dataWithoutMissingValue = pd.DataFrame()
        else:
            dataWithoutMissingValue = self.__categoryColumnMissingValueImputing(data, dataProfile, methodDefault,
                                                                                dictOfMethod)

        dicOutput = dict()
        dicOutput['dataWithoutMissingValue'] = dataWithoutMissingValue

        return dicOutput

    def __categoryColumnMissingValueImputing(self, data, dataProfile, methodDefault, dictOfMethod):
        """
        填充类别列缺失值
        :param data: 待处理的数据
        :param dataProfile: 数据列数据画像
        :param methodDefault: 缺省处理方法
        :param dictOfMethod: 处理方法字典
        :return: 无缺失值的数据
        """
        # TODO: 与外部调用判断逻辑重复，考虑清理
        print(data.shape[1])
        if data.shape[1] == 0:
            return data.copy()

        dataProfile = dataProfile.set_index(dataProfile['col_name'])
        # 循环各列
        for colName in data.columns:
            # 匹配列缺失函数
            method = dictOfMethod.get(colName)
            if method is None:
                method = methodDefault
            # 获取填充值
            if method == 'mode':
                str = dataProfile.loc[colName, 'mode']
            elif method.startswith('const_'):
                # 空值填充为指定的常数值
                str = method[6:]
            else:
                # TODO: 只使用mode方法填充空值，methodDefault是否还需要？考虑清理
                raise Exception("error method")

            data[colName] = data[colName].fillna(str)
        data = data.compute()
        return data
