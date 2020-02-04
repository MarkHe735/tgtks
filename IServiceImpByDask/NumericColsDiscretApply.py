#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : NumericColsDiscretApply.py
@ Author: WangJun
@ Date  : 2019-11-26
@ Desc  : 应用分箱规则，对数值列进行分箱

"""
import pandas as pd
from sklearn.preprocessing import KBinsDiscretizer
#from feature_engineering import discretization as dc
from IService.DService import DService
from sklearn.externals import joblib

'''
bin_rule = joblib.load("/home/wj/code3/knimeModule/knime20191126/data/TrainData_numeric.m")
idCol = 'id'
labelCol = 'label'

ValueDf = pd.read_csv('/home/wj/code3/knimeModule/knime20191126/data/TrainData_numeric.csv',
                      dtype={'id': int,  'value1': int, 'value2': int})
bin_equal_width = joblib.load("/home/wj/code3/knimeModule/knime20191126/data/TrainData_numeric.m")

#等宽：uniform；等频：quantile；K-means：kmeans
method = 'EqualWidth'
colList = ['value1','value2']
'''

class NumericColsDiscretApply(DService):

    def __init__(self):
        pass

    # 等宽分箱
    def equal_width_apply(self,bin_equal_width,X_train, colList):
        #bin_equal_width = KBinsDiscretizer(n_bins=binningNum, encode='ordinal', strategy='uniform').fit(X_train[colList])
        X_train_width = bin_equal_width.transform(X_train[colList])
        colList_new = [col + '_EqualWidth' for col in colList]
        X_train_width_df = pd.DataFrame(X_train_width, columns=colList_new)
        return X_train_width_df


    # 等频分箱
    def equal_freq_apply(self,bin_equal_freq,X_train, colList):
        #bin_equal_freq = KBinsDiscretizer(n_bins=binningNum, encode='ordinal', strategy='quantile').fit(X_train[colList])
        X_train_freq = bin_equal_freq.transform(X_train[colList])
        colList_new = [col + '_EqualFreq' for col in colList]
        X_train_freq_df = pd.DataFrame(X_train_freq, columns=colList_new)
        return X_train_freq_df

    # K-means
    def kmeans_apply(self,bin_kmeans,X_train, colList):
        #bin_kmeans = KBinsDiscretizer(n_bins=binningNum, encode='ordinal', strategy='kmeans').fit(X_train[colList])
        X_train_Kmeans = bin_kmeans.transform(X_train[colList])
        colList_new = [col + '_Kmeans' for col in colList]
        X_train_Kmeans_df = pd.DataFrame(X_train_Kmeans, columns=colList_new)
        return X_train_Kmeans_df

    '''
    # 卡方分箱
    def Chi_Merge(self,X_train, labelCol, col, binningNum, X_train_copy):
        enc3 = dc.ChiMerge(col=col, num_of_bins=binningNum).fit(X=X_train, y=labelCol)
        ChiMergeTemp = enc3.transform(X_train)
        X_train_copy = pd.concat([X_train_copy, ChiMergeTemp.loc[:, col + '_chimerge']], axis=1)
        return X_train_copy
    '''

    def NumericDiscretApply(self,ValueDf,bin_rule,idCol,method,colList):
        '''
        :param ValueDf: 待分箱数据集
        :param bin_rule: 分箱规则集
        :param idCol: id列
        :param method: 分箱方法（等宽：EqualWidth；等频：EqualFreq；K-means：Kmeans）
        :param colList: 需要分箱的列，list格式，如：['col2','col3']
        :return: 应用分箱后的数据集
        '''
        if method == 'EqualWidth':
            ValueDf_bin = self.equal_width_apply( bin_rule,ValueDf, colList)
        elif method == 'EqualFreq':
            ValueDf_bin = self.equal_freq_apply(bin_rule,ValueDf, colList)
        elif method == 'Kmeans':
            ValueDf_bin = self.kmeans_apply(bin_rule,ValueDf, colList)
        ValueDf_bin = pd.concat([ValueDf_bin,ValueDf[idCol]],axis=1)
        return ValueDf_bin


    def process(self, dicInput, dicParams):
        """
        :*功能*: 对数据帧中的数值列进行分箱衍生，生成离散类别列. (2 ==> 1)

        :param dicInput:  <dict> 输入的DataFrame数据表。形式:{'inputDf1':<dataDf>, 'inputDf2':<applyDf>}
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
        bin_rule = inputDf.get('inputDf2')

        # 控制参数
        mapping = dicParams['mapping']
        idCol = mapping.get('idCol')
        method = mapping.get('method')
        colList = mapping.get('colList')

        # 数据处理
        ValueDf_bin = self.NumericDiscretApply(ValueDf, bin_rule, idCol, method, colList)

        # 输出
        #outputDf = inputDf
        dicOutput['outputDf1'] = ValueDf_bin

        return dicOutput