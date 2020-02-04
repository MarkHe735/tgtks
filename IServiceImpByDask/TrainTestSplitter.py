#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : TrainTestSplitter.py
@ Author: LengWei
@ Date  : 2019-11-26
@ Desc  : 对数据帧进行拆分，将数据集拆分成训练集和测试集

"""
from dask.diagnostics import ProgressBar
from dask_ml.model_selection import train_test_split

from IService.DService import DService


class TrainTestSplitter(DService):

    def __init__(self):
        pass

    def __split_train_test_sets(self, dataframe, labelCol, testRatio):
        """
        按照拆分比例，把数据集拆分成训练集和测试集（数据列数与原数据集相同），
        同时保持训练集和测试集的0/1分布与原始数据的0/1分布相同
        :param dataframe: 待划分的数据集
        :param labelCol: 标签列
        :param testRatio: 划分测试集比例
        :return: 划分完成的训练集、测试集
        """
        # split the label and X columns
        X_cols = list(set(dataframe.columns.tolist()) - {labelCol})
        X = dataframe.loc[:, X_cols]
        y = dataframe.loc[:, labelCol]

        # invoke train_test_split to do train and test set splitting.
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=float(testRatio), random_state=0)

        # combine the label column and X columns
        X_train[labelCol] = y_train
        X_test[labelCol] = y_test
        with ProgressBar():
            X_train = X_train.compute()
            X_test = X_test.compute()

        return X_train, X_test

    def process(self, dicInput, dicParams):
        """
        :*功能*: 根据引用帧对数据帧的列的进行筛选，保留/去除在引用帧中的列. (2 ==> 2)

        :param dicInput:  <dict> 输入的DataFrame数据表。形式:{'inputDf':<dataDf>}
        :param dicParams:  （类似yaml格式）
                - mapping : <dict> 必选。
                    { 列名：填充值} 的字典，填充值支持函数形式。
                - ratio:

        :*示例*:
            ::
               CP08:
                  mapping: {"MONTH_ON_BOOK":-1, "EDUCA":4, "OCC_CATGRY":5, "YR_IN_COMP":0}  # support functions
                  outfile: "BaseFeatureDerive_CP08"

        :return: dicOutput :  <dict> 输出的DataFrame数据表。形式: {'outputDf1': <trainDf>, 'outputDf2': <testDf>}
        """
        dicOutput = dict()

        # 输入数据
        inputDf = dicInput['inputDf']

        # 控制参数
        mapping = dicParams['mapping']

        labelCol = mapping['labelCol']
        testRatio = mapping['testRatio']

        # 数据处理
        X_train, X_test = self.__split_train_test_sets(inputDf, labelCol, testRatio)

        # 输出
        dicOutput['outputDf1'] = X_train
        dicOutput['outputDf2'] = X_test

        return dicOutput
