#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : NumericColsDiscret.py
@ Author: WangJun
@ Date  : 2019-11-26
@ Desc  : 对数值列进行离散化衍生，生成类别列

"""
import pandas as pd
from sklearn.preprocessing import KBinsDiscretizer
# from feature_engineering import discretization as dc
from IService.DService import DService
from sklearn.externals import joblib

'''
joblib.dump(bin_equal_width, '/home/wj/code3/knimeModule/knime20191126/data/TrainData_numeric.m')

bin_equal_width_1 = joblib.load("/home/wj/code3/knimeModule/knime20191126/data/TrainData_numeric.m")
ValueDf_3 = bin_equal_width_1.transform(ValueDf[ColsAll_filter])


idCol = 'id'
labelCol = 'label'

ValueDf = pd.read_csv('/home/wj/code3/knimeModule/knime20191126/data/TrainData.csv',
                      dtype={'id': int,  'value1': int, 'value2': int})

#等宽：uniform；等频：quantile；K-means：kmeans
method = 'EqualWidth'
binningNum = 3
colList = ['value2']
'''


class NumericColsDiscret(DService):

    def __init__(self):
        pass

    # 等宽分箱
    def equal_width(self, X_train, colList, binningNum):
        bin_equal_width = KBinsDiscretizer(n_bins=binningNum, encode='ordinal', strategy='uniform').fit(
            X_train[colList])
        X_train_width = bin_equal_width.transform(X_train[colList])
        colList_new = [col + '_EqualWidth' for col in colList]
        X_train_width_df = pd.DataFrame(X_train_width, columns=colList_new)
        return X_train_width_df, bin_equal_width

    # 等频分箱
    def equal_freq(self, X_train, colList, binningNum):
        bin_equal_freq = KBinsDiscretizer(n_bins=binningNum, encode='ordinal', strategy='quantile').fit(
            X_train[colList])
        X_train_freq = bin_equal_freq.transform(X_train[colList])
        colList_new = [col + '_EqualFreq' for col in colList]
        X_train_freq_df = pd.DataFrame(X_train_freq, columns=colList_new)
        return X_train_freq_df, bin_equal_freq

    # K-means
    def kmeans(self, X_train, colList, binningNum):
        bin_kmeans = KBinsDiscretizer(n_bins=binningNum, encode='ordinal', strategy='kmeans').fit(X_train[colList])
        X_train_Kmeans = bin_kmeans.transform(X_train[colList])
        colList_new = [col + '_Kmeans' for col in colList]
        X_train_Kmeans_df = pd.DataFrame(X_train_Kmeans, columns=colList_new)
        return X_train_Kmeans_df, bin_kmeans

    '''
    # 卡方分箱
    def Chi_Merge(self,X_train, labelCol, col, binningNum, X_train_copy):
        enc3 = dc.ChiMerge(col=col, num_of_bins=binningNum).fit(X=X_train, y=labelCol)
        ChiMergeTemp = enc3.transform(X_train)
        X_train_copy = pd.concat([X_train_copy, ChiMergeTemp.loc[:, col + '_chimerge']], axis=1)
        return X_train_copy
    '''

    def NumericDiscret(self, ValueDf, idCol, method, colList, binningNum):
        '''
        :param ValueDf: 需要分箱的数据集
        :param idCol: id列
        :param method: 分箱方法（等宽：EqualWidth；等频：EqualFreq；K-means：Kmeans）
        :param colList: 需要分箱的列，list格式，如：['value2']
        :param binningNum: 分箱数量
        :return:分箱后的数据集，分箱规则
        '''
        if method == 'EqualWidth':
            ValueDf_bin, bin_rule = self.equal_width(ValueDf, colList, binningNum)
        elif method == 'EqualFreq':
            ValueDf_bin, bin_rule = self.equal_freq(ValueDf, colList, binningNum)
        elif method == 'Kmeans':
            ValueDf_bin, bin_rule = self.kmeans(ValueDf, colList, binningNum)
        ValueDf_bin = pd.concat([ValueDf_bin, ValueDf[idCol]], axis=1)
        return ValueDf_bin, bin_rule

    def process(self, dicInput, dicParams):
        """
        :*功能*: 对数据帧中的数值列进行分箱衍生，生成离散类别列. (1 ==> 1)

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
        ValueDf = inputDf.get('inputDf1')

        # 控制参数
        mapping = dicParams['mapping']
        idCol = mapping.get('idCol')
        method = mapping.get('method')
        colList = mapping.get('colList')
        binningNum = mapping.get('binningNum')

        # 数据处理
        ValueDf_bin, bin_rule = self.NumericDiscret(ValueDf, idCol, method, colList, binningNum)

        # 输出
        # outputDf = inputDf
        dicOutput['outputDf1'] = ValueDf_bin
        dicOutput['outputDf2'] = bin_rule

        return dicOutput


