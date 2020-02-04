#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd
from sklearn.preprocessing import KBinsDiscretizer


class NumberColumnBinningService(DService):
    """
     功能        数值列自动分箱服务

    　输入        待分箱的数据    数据帧类型      dataOfNumberColumn             全部为数值列
    　
   　 控制        缺省分箱方法  　字符串类型      defaultBinningMethodList       [EqualFreq_10]      EqualWidth, EqualFreq, KMeans
    　控制        分箱方法字典    字典类型        dictOfBinningMethodList        {}
    　
      输出        分箱模型字典    字典类型        modelOfBinning                 包括模型字典和名称字典

    """

    def process(self, dicInput, dicParams):

        data = dicInput.get('dataOfNumberColumn')
        if isinstance(data, type(None)):
            raise Exception("no input dataOfNumberColumn")

        defaultBinningMethodList = dicParams.get('defaultBinningMethodList')
        if defaultBinningMethodList is None:
            defaultBinningMethodList = ['EqualFreq_10']

        dictOfBinningMethodList = dicParams.get('dictOfBinningMethodList')
        if dictOfBinningMethodList is None:
            dictOfBinningMethodList = dict()

        if (data.shape[1] == 0):
            modelOfBinning = pd.DataFrame()
        else:
            modelOfBinning = self.__numberColumnBinning(data, defaultBinningMethodList, dictOfBinningMethodList)

        dicOutput = dict()
        dicOutput['modelOfBinning'] = modelOfBinning

        return dicOutput

    def __numberColumnBinning(self, data, defaultBinningMethodList, dictOfBinningMethodList):
        """
        数值列自动分箱服务
        :param data: 待分箱的数据
        :param defaultBinningMethodList: 缺省分箱方法
        :param dictOfBinningMethodList: 分箱方法字典
        :return: 分箱模型字典
        """
        dictOfBinningModel = dict()
        dictOfBinningName = dict()
        # 循环各列
        for colName in data.columns:
            # 声明字典value为列表
            dictOfBinningModel[colName] = []
            dictOfBinningName[colName] = []
            # 获取分箱方法
            methodList = dictOfBinningMethodList.get(colName)
            if methodList is None:
                methodList = defaultBinningMethodList
            # 获取 箱数 与 分箱策略
            for method in methodList:
                if method.startswith('EqualWidth_'):
                    m = 'uniform'
                    number = int(method[11:])
                elif method.startswith('EqualFreq_'):
                    m = 'quantile'
                    number = int(method[10:])
                elif method.startswith('KMeans_'):
                    m = 'kmeans'
                    number = int(method[7:])
                else:
                    raise Exception("error method")
                # 创建分箱对象：KBinsDiscretizer
                model = KBinsDiscretizer(n_bins=number, encode='ordinal', strategy=m).fit(data[[colName]])
                # 将分箱对象与分箱方法保存到对应字典
                dictOfBinningModel[colName].append(model)
                dictOfBinningName[colName].append(method)

        modelOfBinning = dict()
        modelOfBinning['dictOfBinningModel'] = dictOfBinningModel
        modelOfBinning['dictOfBinningName'] = dictOfBinningName

        return modelOfBinning
