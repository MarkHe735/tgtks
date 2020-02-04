#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import category_encoders as ce
import pandas as pd


class CategoryColumnEncodingService(DService):
    """
     功能        类别列编码服务

    　输入        待编码的数据    数据帧类型      dataOfCategoryColumn        全部为类别列
    　
   　 控制        缺省编码方法  　字符串类型      defaultEncodingMethod       onehot      onehot, count, freq
    　控制        编码方法字典    字典类型        dictOfEncodingMethod        {}

     输出         编码模型        字典类型        modelOfEncoding

    onehot 编码的次序是按照类别取值出现频率由高到低排序，频率最高为１，以此类推

    """

    def process(self, dicInput, dicParams):

        data = dicInput.get('dataOfCategoryColumn')
        if isinstance(data, type(None)):
            raise Exception("no input dataOfCategoryColumn")

        defaultEncodingMethod = dicParams.get('defaultEncodingMethod')
        if defaultEncodingMethod is None:
            defaultEncodingMethod = 'onehot'

        dictOfEncodingMethod = dicParams.get('dictOfEncodingMethod')
        if dictOfEncodingMethod is None:
            dictOfEncodingMethod = dict()

        if (data.shape[1] == 0):
            modelOfEncoding = pd.DataFrame()
        else:
            modelOfEncoding = self.__categoryColumnEncoding(data, defaultEncodingMethod, dictOfEncodingMethod)

        dicOutput = dict()
        dicOutput['modelOfEncoding'] = modelOfEncoding

        return dicOutput

    def __categoryColumnEncoding(self, data, defaultEncodingMethod, dictOfEncodingMethod):
        """
        类别列编码服务
        :param data: 待编码的数据
        :param defaultEncodingMethod: 缺省编码方法
        :param dictOfEncodingMethod: 编码方法字典
        :return: 编码模型
        """
        dictOfEncodingModel = dict()
        dictOfEncodingName = dict()
        # 循环各列
        for colName in data.columns:
            # 获取分箱方法
            method = dictOfEncodingMethod.get(colName)
            if method is None:
                method = defaultEncodingMethod
            # 获取 分箱对象 与 分箱名
            dictOfEncodingName[colName] = method
            if method == 'onehot':
                onehot_enc = ce.OneHotEncoder(cols=[colName]).fit(data[colName])
                dictOfEncodingModel[colName] = onehot_enc
            elif method == 'count':
                vcs = data[colName].value_counts()
                dictOfEncodingModel[colName] = vcs
            elif method == 'freq':
                vcs = data[colName].value_counts()
                dictOfEncodingModel[colName] = vcs
            else:
                raise Exception("error method")

        modelOfEncoding = dict()
        modelOfEncoding['dictOfEncodingModel'] = dictOfEncodingModel
        modelOfEncoding['dictOfEncodingName'] = dictOfEncodingName

        return modelOfEncoding
