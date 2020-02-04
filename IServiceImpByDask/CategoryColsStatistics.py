#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : CategoryColsStatistics.py
@ Author: WangJun
@ Date  : 2019-12-05
@ Desc  : 类别列数值统计：类别列对数值列按照统计规则做计算

"""
import featuretools as ft
import pandas as pd

from IService.DService import DService


class CategoryColsStatistics(DService):

    def __init__(self):
        pass

    def __catetory_num_statistics(self, df, category_col, num_cols, id_col, statistics_methods):
        """
        对输入数据集，类别列对数值列按照统计规则做计算（统计规则：mean、max、min、count、sum、std），如生成男性平均身高
        :param df:
        :param category_col:
        :param num_cols:
        :param id_col:
        :param statistics_methods:
        :return:
        """
        base_entity = "data_set"
        dataframe = df[[id_col, category_col] + num_cols]
        es = ft.EntitySet(id="original")
        es = es.entity_from_dataframe(entity_id=base_entity,
                                      dataframe=dataframe,
                                      index=id_col)
        es = es.normalize_entity(base_entity_id=base_entity,
                                 new_entity_id=category_col,
                                 index=category_col)
        feature_matrix, feature_defs = ft.dfs(entityset=es,
                                              target_entity=category_col,
                                              agg_primitives=statistics_methods,
                                              trans_primitives=[],
                                              max_depth=2)
        # dataframe = pd.merge(dataframe, feature_matrix, how='left', on=category_col)
        # dataframe.drop([category_col] + num_cols, axis=1, inplace=True)
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
        category_col = mapping['category_col']
        num_cols = mapping['num_cols']
        id_col = mapping['id_col']
        statistics_methods = mapping['statistics_methods']

        outputDf = self.__catetory_num_statistics(inputDf, category_col, num_cols, id_col, statistics_methods)

        # 输出
        dicOutput['outputDf'] = outputDf

        return dicOutput
