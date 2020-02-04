#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import category_encoders as ce
import pandas as pd


class SupervisedCategoryColumnEncodingService(DService):
    """
     功能        有监督类别列编码服务

    　输入       待编码的数据    数据帧类型      dataOfCategoryAndLabelColumn

      控制       表前列名称  　  字符串类型      colnameOfLabelColumn            label
   　 控制       缺省编码方法  　字符串类型      defaultEncodingMethodList       ['catboost']      catboost,target,woe
    　控制       编码方法字典    字典类型        dictOfEncodingMethodList        {}

     输出        编码模型        字典类型        modelOfEncoding

    onehot编码的次序是按照类别取值出现频率由高到低排序，频率最高为１，以此类推

    """

    def process(self, dicInput, dicParams):

        data = dicInput.get('dataOfCategoryAndLabelColumn')
        if isinstance(data, type(None)):
            raise Exception("no input dataOfCategoryAndLabelColumn")

        colnameOfLabelColumn = dicParams.get('colnameOfLabelColumn')
        if colnameOfLabelColumn is None:
            colnameOfLabelColumn = 'label'

        defaultEncodingMethodList = dicParams.get('defaultEncodingMethodList')
        if defaultEncodingMethodList is None:
            defaultEncodingMethodList = ['catboost']

        dictOfEncodingMethodList = dicParams.get('dictOfEncodingMethodList')
        if dictOfEncodingMethodList is None:
            dictOfEncodingMethodList = dict()

        if (data.shape[1] == 0):
            modelOfEncoding = pd.DataFrame()
        else:
            modelOfEncoding = self.__categoryColumnEncoding(data, colnameOfLabelColumn, defaultEncodingMethodList,
                                                            dictOfEncodingMethodList)

        dicOutput = dict()
        dicOutput['modelOfEncoding'] = modelOfEncoding

        return dicOutput

    def __categoryColumnEncoding(self, data, colnameOfLabelColumn, defaultEncodingMethodList, dictOfEncodingMethodList):

        dictOfEncodingModel = dict()
        dictOfEncodingName = dict()

        y = data.pop(colnameOfLabelColumn)
        X = data
        for colName in X.columns:
            # 初始化字典value为列表
            dictOfEncodingModel[colName] = []
            dictOfEncodingName[colName] = []
            # 获取类别编码方法列表
            methodList = dictOfEncodingMethodList.get(colName)
            if methodList is None:
                methodList = defaultEncodingMethodList

            # 循环各编码方法
            for method in methodList:
                dictOfEncodingName[colName].append(method)
                if method == 'catboost':
                    encoder = ce.CatBoostEncoder()
                    encoder.fit(X[colName], y)
                    dictOfEncodingModel[colName].append(encoder)
                elif method == 'target':
                    encoder = ce.TargetEncoder()
                    encoder.fit(X[colName], y)
                    dictOfEncodingModel[colName].append(encoder)
                elif method == 'woe':
                    encoder = ce.WOEEncoder()
                    encoder.fit(X[colName], y)
                    dictOfEncodingModel[colName].append(encoder)
                else:
                    raise Exception("error method")

        modelOfEncoding = dict()
        modelOfEncoding['dictOfEncodingModel'] = dictOfEncodingModel
        modelOfEncoding['dictOfEncodingName'] = dictOfEncodingName

        return modelOfEncoding
