#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : CategoryColsEncode.py
@ Author: WangJun
@ Date  : 2019-11-26
@ Desc  : 对类别列进行编码

"""
import pandas as pd
from IService.DService import DService
import category_encoders as ce
from sklearn.externals import joblib
from sklearn.model_selection import StratifiedKFold

'''
idCol = 'id'
labelCol = 'label'

ClassDf = pd.read_csv('/home/wj/code3/knimeModule/knime20191126/data/TrainData.csv')
#joblib.dump(target_enc, '/home/wj/code3/knimeModule/knime20191126/data/target_enc.m')

K = 3

#one-hot编码：OneHot；序列编码：Ordinal；目标编码：Target；WOE编码：WOE
method = 'OneHot'
colList = ['col2','col3']
'''
class CategoryColsEncode_K(DService):

    def __init__(self):
        pass

    # 目标编码
    def Target_encoding(self, X_train, X_test,labelCol, colList):
        target_enc = ce.TargetEncoder(cols=colList).fit(X_train, X_train[labelCol])
        target_Df = target_enc.transform(X_test)
        colList_new = []
        for col in target_Df.columns.tolist():
            if col in colList:
                colList_new.append(col + '_Target')
            else:
                colList_new.append(col)
        target_Df.columns = colList_new
        return target_Df, target_enc

    # WOE编码
    def WOE_encoding(self, X_train, X_test, labelCol, colList):
        woe_enc = ce.WOEEncoder(cols=colList).fit(X_train, X_train[labelCol])
        woe_Df = woe_enc.transform(X_test)
        colList_new = []
        for col in woe_Df.columns.tolist():
            if col in colList:
                colList_new.append(col + '_WOE')
            else:
                colList_new.append(col)
        woe_Df.columns = colList_new
        return woe_Df, woe_enc


    def CategoryEncode(self,ClassDf,labelCol,method,colList,K):
        '''
        :param ClassDf: 离散化后都是类别的数据集
        :param labelCol: label标签名
        :param method: 编码方式（one-hot编码：OneHot；序列编码：Ordinal；目标编码：Target；WOE编码：WOE）
        :param colList: 需要分箱的列，list格式，如：['col2','col3']
        :param K: K折交叉验证
        :return: 编码后的数据集，编码规则
        '''
        if K >= 2 and isinstance(K,int):
            cols = list(set(ClassDf.columns.tolist()) - set([labelCol]))
            sfolder = StratifiedKFold(n_splits=K, random_state=0, shuffle=False)
            X = ClassDf[cols]
            y = ClassDf[labelCol]
            i = 0
            test_Df_All = pd.DataFrame()
            train_enc_dict = {}
            for train, test in sfolder.split(X, y):
                X_train = ClassDf.loc[train, :]
                X_test = ClassDf.loc[test, :]
                if method == 'Target':
                    test_Df, train_enc = self.Target_encoding(X_train, X_test, labelCol, colList)
                elif method == 'WOE':
                    test_Df, train_enc = self.WOE_encoding(X_train, X_test, labelCol, colList)
                test_Df_All = pd.concat([test_Df_All, test_Df], axis=0)
                train_enc_dict[i] = train_enc
                i = i + 1
        else:
            print('Please enter an integer greater than 1')
        return test_Df_All, train_enc_dict

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
        ClassDf = inputDf.get('inputDf1')

        # 控制参数
        mapping = dicParams['mapping']
        labelCol = mapping.get('labelCol')
        method = mapping.get('method')
        colList = mapping.get('colList')
        K = mapping.get('K')

        # 数据处理
        ClassDf_enc, enc_rule = self.CategoryEncode(ClassDf, labelCol, method, colList,K)

        # 输出
        #outputDf = inputDf
        dicOutput['outputDf1'] = ClassDf_enc
        dicOutput['outputDf2'] = enc_rule

        return dicOutput