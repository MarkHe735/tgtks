#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import category_encoders as ce
import pandas as pd


class KFlodSupervisedCategoryColumnEncodingService(DService):
    """
     功能        有监督类别列编码服务

    　输入        待编码的数据     数据帧类型      　dataOfCategoryAndLabelColumn

      控制        标签列名称  　   字符串类型        colnameOfLabelColumn            label
   　 控制        缺省编码方法  　 字符串类型        defaultEncodingMethodList       ['catboost']      catboost,target,woe
    　控制        编码方法字典     字典类型          dictOfEncodingMethodList        {}
      控制        折数             整数类型          k                               5

     输出         编码模型         字典类型           modelOfEncoding

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

        k = dicParams.get('k')
        if k is None:
            k = 10

        if (data.shape[1] == 0):
            modelOfEncoding = pd.DataFrame()
        else:
            modelOfEncoding = self.__categoryColumnEncoding(data, colnameOfLabelColumn, defaultEncodingMethodList,
                                                            dictOfEncodingMethodList, k)

        dicOutput = dict()
        dicOutput['modelOfEncoding'] = modelOfEncoding

        return dicOutput

    def __categoryColumnEncoding(self, data, colnameOfLabelColumn, defaultEncodingMethodList, dictOfEncodingMethodList,
                                 k):
        """
        有监督类别列编码
        :param data: 待编码的数据
        :param colnameOfLabelColumn: 标签列名称
        :param defaultEncodingMethodList: 缺省编码方法
        :param dictOfEncodingMethodList: 编码方法字典
        :param k: 折数
        :return: 编码模型
        """
        dictOfEncodingModel = dict()
        dictOfEncodingName = dict()

        y = data.pop(colnameOfLabelColumn)
        X = data
        y = y.reset_index(drop=True)
        X = X.reset_index(drop=True)
        # 循环各列
        for colName in X.columns:

            dictOfEncodingModel[colName] = []
            dictOfEncodingName[colName] = []

            X_col = X[colName]
            methodList = dictOfEncodingMethodList.get(colName)

            if methodList is None:
                methodList = defaultEncodingMethodList

            for method in methodList:
                dictOfEncodingName[colName].append(method)
                encoder_list = []
                for i in range(k):
                    if method == 'catboost':
                        encoder = ce.CatBoostEncoder()
                        encoder.fit(X_col[X_col.index % k != i], y[y.index % k != i])
                        encoder_list.append(encoder)
                    elif method == 'target':
                        encoder = ce.TargetEncoder(smoothing=3)
                        encoder.fit(X_col[X_col.index % k != i], y[y.index % k != i])
                        encoder_list.append(encoder)
                    elif method == 'woe':
                        encoder = ce.WOEEncoder()
                        encoder.fit(X_col[X_col.index % k != i], y[y.index % k != i])
                        encoder_list.append(encoder)
                    else:
                        raise Exception("error method")
                dictOfEncodingModel[colName].append(encoder_list)

        modelOfEncoding = dict()
        modelOfEncoding['dictOfEncodingModel'] = dictOfEncodingModel
        modelOfEncoding['dictOfEncodingName'] = dictOfEncodingName

        return modelOfEncoding
