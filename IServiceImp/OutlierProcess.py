#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : OuterlierProcess.py
@ Author: LengWei
@ Date  : 2019-11-26
@ Desc  : 数据帧异常值处理，对数据列的异常值进行修正

"""
import json
import numpy as np

from IService.DService import DService


class OutlierProcess(DService):

    def __init__(self):
        pass

    def __fill_outlier(self, col, method):
        """
        异常值填充
        :param col:
        :param method:
        :return:
        """
        if method.lower() == "iqr":
            v_low = self.prefileDf[self.prefileDf['Columns'] == col]['Q1-1.5IQR']
            v_up = self.prefileDf[self.prefileDf['Columns'] == col]['Q3+1.5IQR']
            fill_low_value = v_low.values[0]
            fill_up_value = v_up.values[0]
            self.df[col][self.df[col] < fill_low_value] = fill_low_value
            self.df[col][self.df[col] > fill_up_value] = fill_up_value
        elif method.lower() == "sigma":
            v_low = self.prefileDf[self.prefileDf['Columns'] == col]['Mean-3sigma']
            v_up = self.prefileDf[self.prefileDf['Columns'] == col]['Mean+3sigma']
            fill_low_value = v_low.values[0]
            fill_up_value = v_up.values[0]
            self.df[col][self.df[col] < fill_low_value] = fill_low_value
            self.df[col][self.df[col] > fill_up_value] = fill_up_value
        return self.df

    def __outlier_pro(self, colsMap, point):
        """
        根据控制参数，实现对异常值的修改
        :param df:
        :param prefileDf:
        :param pro_dict:
        :return:
        """
        for col in self.df.columns:
            if col in colsMap:
                self.df = self.__fill_outlier(col, colsMap[col])
                continue
            # 判断正太分布相似度
            else:
                json_str = self.prefileDf.loc[self.prefileDf['Columns'] == col, 'NormalCorr'].tolist()[0]
                if json_str is np.nan:
                    self.df = self.__fill_outlier(col, 'iqr')
                    continue
                normal_corr = json.loads(json_str)
                if normal_corr['statistic'] is np.nan:
                    self.df = self.__fill_outlier(col, 'iqr')

                significance_level = normal_corr['significance_level']
                critical_values_index = significance_level.index(point)
                critical_value = normal_corr['critical_values'][critical_values_index]
                if normal_corr['statistic'] < critical_value:
                    self.df = self.__fill_outlier(col, 'sigma')
                else:
                    self.df = self.__fill_outlier(col, 'iqr')
        return self.df

    def process(self, dicInput, dicParams):
        """
        :*功能*: 对数据帧的数据列的异常值进行修正. (2 ==> 2)

        :param dicInput:  <dict> 输入的DataFrame数据表。形式:{'inputDf1':<dataDf>, 'inputDf2':<profileDf>}
        :param dicParams:  （类似yaml格式）
                - mapping : <dict> 必选。
                    { 列名：填充值} 的字典，填充值支持函数形式。
                -

        :*示例*:
            ::
               CP08:
                  mapping: {"MONTH_ON_BOOK":-1, "EDUCA":4, "OCC_CATGRY":5, "YR_IN_COMP":0}  # support functions
                  outfile: "BaseFeatureDerive_CP08"

        :return: dicOutput :  <dict> 输出的DataFrame数据表。形式: {'outputDf1': <dataDf>, 'outputDf2': <applyDf>}
        """
        dicOutput = dict()

        # 输入数据
        inputDf = dicInput['inputDf1']
        prefileDf = dicInput['inputDf2']

        # 控制参数
        mapping = dicParams['mapping']
        colsMap = mapping['colsMap']
        point = mapping['point']

        # 数据处理
        self.df = inputDf
        self.prefileDf = prefileDf
        outputDf = self.__outlier_pro(colsMap, point)

        # 输出
        dicOutput['outputDf'] = outputDf

        return dicOutput
