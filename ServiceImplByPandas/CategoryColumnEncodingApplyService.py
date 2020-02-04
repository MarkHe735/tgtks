#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd


class CategoryColumnEncodingApplyService(DService):
    """
     功能        类别列编码应用服务

    　输入        待编码的数据    数据帧类型      　 dataOfCategoryColumn            全部为类别列
    　输入        编码模型字典    字典类型           modelOfEncoding

    　输出        编码后的数据    数据帧类型         dataEncoded

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
        else:
            dataEncoded = self.categoryColumnEncodingApply(data, modelOfEncoding)

        dicOutput = dict()
        dicOutput['dataEncoded'] = dataEncoded

        return dicOutput

    def categoryColumnEncodingApply(self, data, modelOfEncoding):
        """
        类别列编码应用服务
        :param data: 待编码的数据
        :param modelOfEncoding: 编码模型字典
        :return: 编码后的数据
        """
        # TODO: 判断与函数外调用逻辑重复，建议清理
        if (data.shape[1] == 0):
            return data
        # 获取分箱对象
        dictOfEncodingModel = modelOfEncoding['dictOfEncodingModel']
        # 获取分箱方法名
        dictOfEncodingName = modelOfEncoding['dictOfEncodingName']
        df_list = []
        # 循环各列
        for colName in data.columns:
            # 获取列对应的分箱对象和名称
            model = dictOfEncodingModel.get(colName)
            method = dictOfEncodingName.get(colName)
            # 执行编码
            if method == 'onehot':
                onehot_Df = model.transform(data[colName])
                columns_new = []
                for col in onehot_Df.columns:
                    columns_new.append(col + '__onehot')
                onehot_Df.columns = columns_new
                df_list.append(onehot_Df)
            elif method == 'count':
                df = pd.DataFrame()
                # 应用value_counts()
                df[colName + '__count'] = data[colName].apply(lambda x: str(model[x]))
                df_list.append(df)
            elif method == 'freq':
                data_encoded_df = data[[colName]].replace(model).fillna(0)
                count = model.values.sum()
                # 频次计算
                data_encoded_df[colName] = data_encoded_df.apply(lambda x: x * 1.0 / count)
                data_encoded_df.columns = [colName + '__freq']
                df_list.append(data_encoded_df)
            else:
                raise Exception("error method")

        dataEncoded = pd.concat(df_list, axis=1)
        return dataEncoded
