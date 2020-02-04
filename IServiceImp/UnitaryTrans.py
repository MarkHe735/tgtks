#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : UnitaryTrans.py
@ Author: WangJun
@ Date  : 2019-12-05
@ Desc  : 一元特征变换：按照规定列规定方法输出变换后的数据表（变换方法：ln、ln(1+p)、square、sqrt）

"""
import numpy as np
from IService.DService import DService


class UnitaryTrans(DService):

    def __init__(self):
        pass

    def __unitary_trans(self, df, col_method_list):
        """
        对输入数据集，按照规定列规定方法输出变换后的数据表（变换方法：ln、ln(1+p)、square、sqrt）
        :param df:
        :param col_method_list:
        :return:
        """
        for col, method in col_method_list.items():
            if method.lower() == "ln":
                df[col + "_LN"] = np.log(df[col])
                df = df.drop([col], axis=1)
            elif method.lower() == "ln(1+p)":
                df[col + "_LN(1+P)"] = np.log1p(df[col])
                df = df.drop([col], axis=1)
            elif method.lower() == "square":
                df[col + "_SQUARE"] = np.square(df[col])
                df = df.drop([col], axis=1)
            elif method.lower() == "sqrt":
                df[col + "_SQRT"] = np.sqrt(df[col])
                df = df.drop([col], axis=1)
        return df

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
        outputDf = self.__unitary_trans(inputDf, mapping)

        # 输出
        dicOutput['outputDf'] = outputDf

        return dicOutput
