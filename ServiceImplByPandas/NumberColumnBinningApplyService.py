#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd


class NumberColumnBinningApplyService(DService):
    """
     功能        数值列自动应用分箱服务

    　输入        待分箱的数据     数据帧类型      　dataOfNumberColumn    全部为数值列
    　输入        分箱模型字典     字典类型          modelOfBinning

    　输出        分箱后的数据     数据帧类型        dataBinned

    """

    def process(self, dicInput, dicParams):

        data = dicInput.get('dataOfNumberColumn')
        if isinstance(data, type(None)):
            raise Exception("no input dataOfNumberColumn")

        modelOfBinning = dicInput.get('modelOfBinning')
        if isinstance(modelOfBinning, type(None)):
            raise Exception("no input modelOfBinning")

        if (data.shape[1] == 0):
            dataBinned = pd.DataFrame()
        else:
            dataBinned = self.__numberColumnBinningApply(data, modelOfBinning)

        dicOutput = dict()
        dicOutput['dataBinned'] = dataBinned

        return dicOutput

    def __numberColumnBinningApply(self, data, modelOfBinning):
        """
        数值列自动应用分箱服务
        :param data: 待分箱的数据
        :param modelOfBinning: 分箱模型字典
        :return: 分箱后的数据
        """
        # 获取分箱对象：KBinsDiscretizer
        dictOfBinningModel = modelOfBinning['dictOfBinningModel']
        # 获取分箱方法名
        dictOfBinningName = modelOfBinning['dictOfBinningName']
        df_list = []
        # 循环各列
        for colName in data.columns:
            # 获取分箱对象和名称列表
            modelList = dictOfBinningModel.get(colName)
            nameList = dictOfBinningName.get(colName)
            # TODO: 如果modelList为None，len()方法将报错，应该做空值判断。
            length = len(modelList)
            # 循环模型列表
            for i in range(length):
                model = modelList[i]
                name = nameList[i]
                # 执行分箱
                data_binned = model.transform(data[[colName]])
                data_binned_df = pd.DataFrame(data_binned, columns=[colName + "_" + name])
                # 添加后缀：S，分箱后转类别
                # maybe slow
                data_binned_df[colName + "_" + name] = data_binned_df[colName + "_" + name].map(lambda x: str(x) + 'S')
                # 收集分箱数据
                df_list.append(data_binned_df)
        dataBinned = pd.concat(df_list, axis=1)
        return dataBinned
