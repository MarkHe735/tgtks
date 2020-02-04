#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : DataProfile.py
@ Author: LengWei
@ Date  : 2019-11-26
@ Desc  : 数据探查，生成数据描述

"""
import json

import dask
import pandas as pd
import dask.dataframe as dd
import dask.array as da
import numpy as np
from scipy import stats
from scipy.stats import anderson


from IService.DService import DService


class DataProfile(DService):

    def __init__(self):
        pass

    def __data_profile(self, dataframe):
        """
        有待改进为：for 循环计算各个指标值，而不是循环列。这样可以集中调用compute函数，进行并行计算。

        计算所给数据集的每列的缺失率、唯一值的比例、常数列比例、最大值、最小值、均值、中位数、众数、0值数量、
        均值-3σ、均值+3σ、均值-2σ、均值+2σ、Q1-1.5*IQR、Q3+1.5*IQR、
        正态分布相似度、log正态分布相似度、均值分布相似度、指数分布相似度
        计算思路：生成二维列表——>字典——>dataframe
        :param dataframe: 数据表
        :return: 数据探索生成的数据表
        """
        dataType = dataframe.dtypes
        dataTypeDf = pd.DataFrame({'Columns': dataType.index, 'Dtypes': dataType.values})

        # 将列分为数值型与字符串型（未来需考虑时间格式）
        col_types = dataTypeDf.loc[:, 'Dtypes']
        colNum = dataTypeDf.loc[(col_types != 'object')
                                & (col_types != 'category')
                                & (col_types != 'datetime'), 'Columns'].to_list()
        colStr = dataTypeDf.loc[col_types == 'object', 'Columns'].tolist()

        empyt_value = ''
        value_dict = {'Columns': [], 'MissRate': [], 'UniqueRate': [], 'ConstantRate': [], 'Max': [], 'Min': [],
                      'Mean': [], 'Median': [], 'Mode': [], 'ZeroNum': [], 'Mean-3sigma': [], 'Mean+3sigma': [],
                      'Mean-2sigma': [], 'Mean+2sigma': [], 'Q1-1.5IQR': [], 'Q3+1.5IQR': [], 'NormalCorr': [],
                      'LogNormalCorr': [], 'UniformCorr': [], 'ExpCorr': []}

        threshold = 1.5
        data_len = dataframe.index.size.compute()
        compare_normal = np.random.uniform(0, 1, data_len)
        for col in dataframe.columns:
            value_dict['Columns'].append(col)
            value_dict['MissRate'].append(dataframe[col].isnull().mean().compute())
            value_dict['UniqueRate'].append((dataframe[col].value_counts().size / dataframe.index.size).compute())
            value_dict['ConstantRate'].append((dataframe[col].value_counts().compute().reset_index(drop=True)[0] / data_len))
            if col in colNum:
                value_dict['Max'].append(np.max(dataframe[col]).compute())
                value_dict['Min'].append(np.min(dataframe[col]).compute())
                value_dict['Mean'].append(np.mean(dataframe[col]).compute())
                value_dict['Median'].append(np.median(dataframe[col]))
                value_dict['ZeroNum'].append(len(dataframe[col][dataframe[col] == 0]))

                mean = dataframe[col].mean().compute()
                std = dataframe[col].std().compute()
                value_dict['Mean-3sigma'].append(mean - 3 * std)
                value_dict['Mean+3sigma'].append(mean + 3 * std)
                value_dict['Mean-2sigma'].append(mean - 2 * std)
                value_dict['Mean+2sigma'].append(mean + 2 * std)

                Q1 = dataframe[col].quantile(0.25).compute()
                Q3 = dataframe[col].quantile(0.75).compute()
                IQR = Q3 - Q1
                value_dict['Q1-1.5IQR'].append(Q1 - (IQR * threshold))
                value_dict['Q3+1.5IQR'].append(Q3 + (IQR * threshold))

                statistic, critical_values, significance_level = anderson(dataframe[col], 'norm')
                value_dict['NormalCorr'].append(
                    json.dumps({'statistic': statistic, 'critical_values': critical_values.tolist(),
                                'significance_level': significance_level.tolist()}))
                statistic, critical_values, significance_level = anderson(np.log(dataframe[col]), 'norm')
                value_dict['LogNormalCorr'].append(
                    json.dumps({'statistic': statistic, 'critical_values': critical_values.tolist(),
                                'significance_level': significance_level.tolist()}))

                data_normal = dataframe[col] / dataframe[col].max()
                ''' https://cloud.tencent.com/developer/article/1530349 '''
                # JS散度
                M = (compare_normal + data_normal.compute()) / 2
                UniformCorr = 0.5 * stats.entropy(compare_normal, M) + 0.5 * stats.entropy(data_normal, M)
                # KL散度
                # UniformCorr = stats.entropy(compare_normal, data_normal)
                value_dict['UniformCorr'].append(UniformCorr)

                statistic, critical_values, significance_level = anderson(dataframe[col], 'expon')
                value_dict['ExpCorr'].append(
                    json.dumps({'statistic': statistic, 'critical_values': critical_values.tolist(),
                                'significance_level': significance_level.tolist()}))


            else:
                value_dict['Max'].append(empyt_value)
                value_dict['Min'].append(empyt_value)
                value_dict['Mean'].append(empyt_value)
                value_dict['Median'].append(empyt_value)
                value_dict['ZeroNum'].append(empyt_value)

                value_dict['Mean-3sigma'].append(empyt_value)
                value_dict['Mean+3sigma'].append(empyt_value)
                value_dict['Mean-2sigma'].append(empyt_value)
                value_dict['Mean+2sigma'].append(empyt_value)

                value_dict['Q1-1.5IQR'].append(empyt_value)
                value_dict['Q3+1.5IQR'].append(empyt_value)

                value_dict['NormalCorr'].append(empyt_value)
                value_dict['LogNormalCorr'].append(empyt_value)
                value_dict['UniformCorr'].append(empyt_value)
                value_dict['ExpCorr'].append(empyt_value)

            if col in colStr:
                value_dict['Mode'].append(stats.mode(dataframe[col])[0][0])
            else:
                value_dict['Mode'].append(empyt_value)

        DataExploration = pd.DataFrame(value_dict)

        return DataExploration

    def process(self, dicInput, dicParams):
        """
        :*功能*: 对数据帧进行数据探查. (1 ==> 1)

        :param dicInput:  <dict> 输入的DataFrame数据表。形式:{'inputDf':<DataFrame>}
        :param dicParams:  （类似yaml格式）
                - mapping : <dict> 必选。
                    { 列名：填充值} 的字典，填充值支持函数形式。
                -

        :*示例*:
            ::
               CP08:
                  mapping: {"MONTH_ON_BOOK":-1, "EDUCA":4, "OCC_CATGRY":5, "YR_IN_COMP":0}  # support functions
                  outfile: "BaseFeatureDerive_CP08"

        :return: dicOutput :  <dict> 输出的DataFrame数据表。形式: {'outputDf': <profileDf>}
        """
        dicOutput = dict()

        # 输入数据
        inputDf = dicInput['inputDf']

        # 控制参数
        mapping = dicParams['mapping']

        # 数据处理
        profile_df = self.__data_profile(inputDf)

        # 输出
        outputDf = profile_df
        dicOutput['outputDf'] = outputDf

        return dicOutput
