#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : DataProfile.py
@ Author: LengWei
@ Date  : 2019-11-26
@ Desc  : 数据探查，生成数据描述

"""
import json
import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats import anderson
from sklearn.preprocessing import normalize

from IService.DService import DService


class DataProfile(DService):

    def __init__(self):
        pass

    def __data_profile(self, dataframe):
        """
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
        colNum = dataTypeDf.loc[dataTypeDf.loc[:, 'Dtypes'] != 'object', 'Columns'].tolist()
        colStr = dataTypeDf.loc[dataTypeDf.loc[:, 'Dtypes'] == 'object', 'Columns'].tolist()

        def calculate_feature(fun, col_name, cal_type='all'):
            col_list = []
            val_list = []
            if cal_type == 'num':
                cols = colNum
            elif cal_type == 'category':
                cols = colStr
            else:
                cols = dataframe.columns
            for col in cols:
                col_list.append(col)
                val_list.append(fun(dataframe[col]))
            res = {'Columns': col_list, col_name: val_list}
            return res

        res_dict = calculate_feature(lambda x: x.isnull().mean(), 'MissRate')
        DataExploration = pd.DataFrame(res_dict)
        res_dict = calculate_feature(lambda x: len(x.value_counts()) / len(dataframe), 'UniqueRate')
        DataExploration = DataExploration.merge(pd.DataFrame(res_dict), on='Columns', how='left')
        res_dict = calculate_feature(lambda x: x.value_counts().reset_index(drop=True)[0] / len(dataframe),
                                     'ConstantRate')
        DataExploration = DataExploration.merge(pd.DataFrame(res_dict), on='Columns', how='left')

        res_dict = calculate_feature(np.max, 'Max', cal_type='num')
        DataExploration = DataExploration.merge(pd.DataFrame(res_dict), on='Columns', how='left')
        res_dict = calculate_feature(np.min, 'Min', cal_type='num')
        DataExploration = DataExploration.merge(pd.DataFrame(res_dict), on='Columns', how='left')
        res_dict = calculate_feature(np.mean, 'Mean', cal_type='num')
        DataExploration = DataExploration.merge(pd.DataFrame(res_dict), on='Columns', how='left')
        res_dict = calculate_feature(np.median, "Median", cal_type='num')
        DataExploration = DataExploration.merge(pd.DataFrame(res_dict), on='Columns', how='left')

        res_dict = calculate_feature(lambda x: stats.mode(x)[0][0], "Mode", cal_type='category')
        DataExploration = DataExploration.merge(pd.DataFrame(res_dict), on='Columns', how='left')
        res_dict = calculate_feature(lambda x: len(x[x == 0]), "ZeroNum", cal_type='num')
        DataExploration = DataExploration.merge(pd.DataFrame(res_dict), on='Columns', how='left')

        # 计算 Mean-3sigma Mean+3sigma Mean-2sigma Mean+2sigma
        col_list = []
        val1_list = []
        val2_list = []
        val3_list = []
        val4_list = []
        for col in colNum:
            mean = dataframe[col].mean()
            std = dataframe[col].std()
            col_list.append(col)
            val1_list.append(mean - 3 * std)
            val2_list.append(mean + 3 * std)
            val3_list.append(mean - 2 * std)
            val4_list.append(mean + 2 * std)
        res_dict = {'Columns': col_list, 'Mean-3sigma': val1_list, 'Mean+3sigma': val2_list,
                    'Mean-2sigma': val3_list, 'Mean+2sigma': val4_list}
        DataExploration = DataExploration.merge(pd.DataFrame(res_dict), on='Columns', how='left')

        # 计算 Q1-1.5IQR，Q3+1.5IQR
        col_list = []
        val1_list = []
        val2_list = []
        threshold = 1.5
        for col in colNum:
            Q1 = dataframe[col].quantile(0.25)
            Q3 = dataframe[col].quantile(0.75)
            IQR = Q3 - Q1
            col_list.append(col)
            val1_list.append(Q1 - (IQR * threshold))
            val2_list.append(Q3 + (IQR * threshold))
        res_dict = {'Columns': col_list, 'Q1-1.5IQR': val1_list, 'Q3+1.5IQR': val2_list}
        DataExploration = DataExploration.merge(pd.DataFrame(res_dict), on='Columns', how='left')

        # 计算分布相似度
        col_list = []
        val1_list = []
        val2_list = []
        val3_list = []
        val4_list = []
        data_len = len(dataframe)
        compare_normal = np.random.uniform(0, 1, data_len)
        for col in colNum:
            col_list.append(col)
            statistic, critical_values, significance_level = anderson(dataframe[col], 'norm')
            val1_list.append(json.dumps({'statistic': statistic, 'critical_values': critical_values.tolist(),
                                         'significance_level': significance_level.tolist()}))
            statistic, critical_values, significance_level = anderson(np.log(dataframe[col]), 'norm')
            val2_list.append(json.dumps({'statistic': statistic, 'critical_values': critical_values.tolist(),
                                         'significance_level': significance_level.tolist()}))

            data_normal = dataframe[col] / dataframe[col].max()
            ''' https://cloud.tencent.com/developer/article/1530349 '''
            # JS散度
            M = (compare_normal + data_normal) / 2
            UniformCorr = 0.5 * stats.entropy(compare_normal, M) + 0.5 * stats.entropy(data_normal, M)
            # KL散度
            # UniformCorr = stats.entropy(compare_normal, data_normal)
            val3_list.append(UniformCorr)

            statistic, critical_values, significance_level = anderson(dataframe[col], 'expon')
            val4_list.append(json.dumps({'statistic': statistic, 'critical_values': critical_values.tolist(),
                                         'significance_level': significance_level.tolist()}))
        res_dict = {'Columns': col_list, 'NormalCorr': val1_list, 'LogNormalCorr': val2_list,
                    'UniformCorr': val3_list, 'ExpCorr': val4_list}
        DataExploration = DataExploration.merge(pd.DataFrame(res_dict), on='Columns', how='left')

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
