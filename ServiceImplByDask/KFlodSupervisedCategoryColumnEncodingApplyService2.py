#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd


class KFlodSupervisedCategoryColumnEncodingApplyService2(DService):
    """
     功能         有监督类别列编码应用服务

    　输入        待编码的数据     数据帧类型    　 dataOfCategoryColumn        全部为类别列
    　输入        编码模型字典     字典类型         modelOfEncoding

    　输出        编码后的数据     数据帧类型       dataEncoded

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

        X = data.reset_index()

        df_list = []
        for colName in data.columns:

            modelList = dictOfEncodingModel.get(colName)
            methodList = dictOfEncodingName.get(colName)

            X_col = X[colName]

            for i in range(len(methodList)):
                model_list = modelList[i]
                method = methodList[i]
                data_encoded_df_flod_list = []
                k = len(model_list)
                for j in range(k):
                    model = model_list[j]
                    data_encoded_df_flod = model.transform(X_col)
                    data_encoded_df_flod.columns = [colName + '__' + method]
                    data_encoded_df_flod_list.append(data_encoded_df_flod)
                data_encoded_df_flod_last = data_encoded_df_flod_list[k - 1]
                for j in range(k - 1):
                    # 行数据相加操作
                    data_encoded_df_flod_last = data_encoded_df_flod_last + data_encoded_df_flod_list[j]
                # 累加后求均值
                data_encoded_df = data_encoded_df_flod_last / k
                df_list.append(data_encoded_df)

        dataEncoded = pd.concat(df_list, axis=1)
        dataEncoded.sort_index(inplace=True)

        dicOutput = dict()
        dicOutput['dataEncoded'] = dataEncoded

        return dicOutput
