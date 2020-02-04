#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd


class SupervisedCategoryColumnEncodingApplyService(DService):
    """
     功能        有监督类别列编码应用服务

    　输入       待编码的数据      数据帧类型      　 dataOfCategoryColumn          全部为类别列
    　输入       编码模型字典      字典类型           modelOfEncoding

    　输出        编码后的数据     数据帧类型         dataEncoded

    """

    def process(self, dicInput, dicParams):

        data = dicInput.get('dataOfCategoryColumn')
        if isinstance(data, type(None)):
            raise Exception("no input dataOfCategoryColumn")

        modelOfEncoding = dicInput.get('modelOfEncoding')
        if type(modelOfEncoding) == type(None):
            raise Exception("no input modelOfEncoding")

        if (data.shape[1] == 0):
            dataEncoded = pd.DataFrame()
            dicOutput = dict()
            dicOutput['dataEncoded'] = dataEncoded
            return dicOutput

        # TODO: 应统一做内部方法封装
        dictOfEncodingModel = modelOfEncoding['dictOfEncodingModel']
        dictOfEncodingName = modelOfEncoding['dictOfEncodingName']

        df_list = []
        # 循环各列
        for colName in data.columns:

            modelList = dictOfEncodingModel.get(colName)
            methodList = dictOfEncodingName.get(colName)
            # 循环方法列表，执行编码方法
            for i in range(len(methodList)):
                model = modelList[i]
                method = methodList[i]
                data_encoded_df = model.transform(data[colName])
                data_encoded_df.columns = [colName + '__' + method]
                df_list.append(data_encoded_df)

        dataEncoded = pd.concat(df_list, axis=1)

        dicOutput = dict()
        dicOutput['dataEncoded'] = dataEncoded

        return dicOutput
