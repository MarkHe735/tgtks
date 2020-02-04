#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ServiceImplByPandas.XGboostGridSearchingService import XGboostGridSearchingService
from IService.DService import DService
import pandas as pd


class XGboostPredictingService(DService):
    """
    功能        XGBoost模型预测

      输入      待预测数据     　 数据帧类型      data
      输入      模型列表     　   模型列表        modelList

    输出        预测数据      　　数据帧类型      predictData

    """

    def process(self, dicInput, dicParams):

        data = dicInput.get('data')
        if isinstance(data, type(None)):
            raise Exception("no input data")

        modelList = dicInput.get('modelList')
        if isinstance(modelList, type(None)):
            raise Exception("no input modelList")

        # TODO: 应统一做内部方法封装
        df_list = []
        for model in modelList:
            y_pred = model.predict_proba(data)
            df_list.append((pd.DataFrame(y_pred))[[1]])
        predict_df = pd.concat(df_list, axis=1)
        predict_df['p'] = predict_df.apply(lambda x: x.mean(), axis=1)

        dicOutput = dict()
        dicOutput['predictData'] = predict_df[['p']]

        return dicOutput


def main():
    data = pd.read_csv("/home/user/tmp/xgboost/UCI_Credit_Card.csv")
    data.pop('ID')

    dic1 = dict()
    dic1['trainData'] = data
    print(data.shape)

    dic2 = dict()
    dic2['colnameOfLabelColumn'] = 'target'

    qps = XGboostGridSearchingService()
    dic3 = qps.process(dic1, dic2)
    modelList = dic3['modelList']

    y = data.pop('target')
    x = data
    dic1 = dict()
    dic1['data'] = x
    dic1['modelList'] = modelList

    qps = XGboostPredictingService()
    dic3 = qps.process(dic1, dic2)
    print(dic3['predictData'])


if __name__ == '__main__':
    main()
