#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : CutOffTwoCategoryColsDateTimeStatistics.py
@ Author: WangJun
@ Date  : 2019-12-05
@ Desc  : 指定时间段内的不同类别时间间隔统计：按照统计时间段切割数据，按照id列和日期列升序排序，按照类别分组，分别计算不同组中相邻日期间的时间间隔，对时间间隔进行统计规则计算（统计规则：mean、max、min、count、sum、std）
"""
import numpy as np
import pandas as pd
import featuretools as ft
from featuretools.primitives import TimeSincePrevious

from IService.DService import DService


class CutOffTwoCategoryColsDateTimeStatistics(DService):

    def __init__(self):
        pass

    def __calculate_cutoff_2_category_time_previous_statistics(self, df, category_col1, category_col2, id_col,
                                                               datetime_col, cutoff_time, time_window,
                                                               statistics_methods):
        """
        按照统计时间段切割数据，按照id列和日期列升序排序，按照类别分组，分别计算不同组中相邻日期间的时间间隔，
        对时间间隔进行统计规则计算（统计规则：mean、max、min、count、sum、std）
        :param df:
        :param category_col1:
        :param category_col2:
        :param id_col:
        :param datetime_col:
        :param statistics_methods:
        :return:
        """
        # 按类别、日期升序排列
        df[datetime_col] = pd.to_datetime(df[datetime_col])
        df.sort_values(by=[category_col1, datetime_col], ascending=[True, True], inplace=True)
        df = df[[id_col, datetime_col, category_col1, category_col2]]
        # 计算各类别记录时间差
        es = ft.EntitySet(id="original")
        es = es.entity_from_dataframe(entity_id="test",
                                      dataframe=df,
                                      index="_NEW_ID",
                                      make_index=True,
                                      time_index=datetime_col,
                                      already_sorted=True)
        date_previous_df, _ = ft.dfs(entityset=es,
                                     target_entity="test",
                                     agg_primitives=[],
                                     trans_primitives=[TimeSincePrevious(unit='days')],
                                     max_depth=2)
        # 将各类别第一个时间差值置为nan
        categories = date_previous_df[category_col1].unique()
        category_df = pd.DataFrame()
        for item in categories:
            sub_df = date_previous_df[date_previous_df[category_col1] == item]
            sub_df.iloc[0, 3] = np.NaN
            category_df = category_df.append(sub_df)
        # 重新赋予时间列，以下用于时间窗计算
        category_df = category_df.reset_index(drop=True)
        category_df = category_df.merge(df[[id_col, datetime_col]], on=id_col, how='left')
        category_df.drop([id_col], axis=1, inplace=True)
        # 生成时间差统计特征
        es = ft.EntitySet(id="categories")
        es = es.entity_from_dataframe(entity_id="category",
                                      dataframe=category_df,
                                      index=id_col,
                                      time_index=datetime_col)

        es = es.normalize_entity(base_entity_id="category",
                                 new_entity_id=category_col1,
                                 make_time_index=False,
                                 index=category_col1)

        es["category"][category_col2].interesting_values = df[category_col2].unique().tolist()
        feature_matrix, _ = ft.dfs(entityset=es,
                                   target_entity=category_col1,
                                   agg_primitives=list(set(["mean"] + statistics_methods)),
                                   where_primitives=["mean"],
                                   trans_primitives=[],
                                   cutoff_time=pd.to_datetime(cutoff_time),
                                   cutoff_time_in_index=True,
                                   training_window=time_window,
                                   max_depth=1)
        return feature_matrix

    def process(self, dicInput, dicParams):
        """
        :*功能*: 对数据帧中的类别列进行编码衍生，生成离散类别列. (1 ==> 1)

        :param dicInput:  <dict> 输入的DataFrame数据表。形式:{'inputDf':<dataDf>}
        :param dicParams:  （类似yaml格式）
                - mapping : <dict> 必选。
                    { 列名：分箱方法名 } 的字典，填充值支持函数形式。
                -

        :*示例*:
            ::
               CP08:
                  mapping: {"MONTH_ON_BOOK":-1, "EDUCA":4, "OCC_CATGRY":5, "YR_IN_COMP":0}  # support functions
                  outfile: "BaseFeatureDerive_CP08"

        :return: dicOutput :  <dict> 输出的DataFrame数据表。形式: {'outputDf1': <dataDf>, 'outputDf2': <transDf>}
        """
        dicOutput = dict()

        # 输入数据
        inputDf = dicInput['inputDf']

        # 控制参数
        mapping = dicParams['mapping']

        # 数据处理
        category_col1 = mapping['category_col1']
        category_col2 = mapping['category_col2']
        id_col = mapping['id_col']
        datetime_col = mapping['datetime_col']
        statistics_methods = mapping['statistics_methods']
        cutoff_time = mapping['cutoff_time']
        time_window = mapping['time_window']
        outputDf = self.__calculate_cutoff_2_category_time_previous_statistics(inputDf, category_col1, category_col2,
                                                                               id_col, datetime_col, cutoff_time,
                                                                               time_window, statistics_methods)

        # 输出
        dicOutput['outputDf'] = outputDf

        return dicOutput
