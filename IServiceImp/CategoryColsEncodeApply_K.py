#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : CategoryColsEncodeApply.py
@ Author: WangJun
@ Date  : 2019-11-26
@ Desc  : 应用编码规则，对类别列进行编码
"""
import pandas as pd
from IService.DService import DService
import category_encoders as ce
from sklearn.externals import joblib
from sklearn.model_selection import StratifiedKFold

'''
labelCol = 'label'

ClassDf = pd.read_csv('/home/wj/code3/knimeModule/knime20191126/data/TrainData.csv')
#joblib.dump(target_enc, '/home/wj/code3/knimeModule/knime20191126/data/target_enc.m')
target_enc_1 = joblib.load("/home/wj/code3/knimeModule/knime20191126/data/target_enc.m")


#one-hot编码：OneHot；序列编码：Ordinal；目标编码：Target；WOE编码：WOE
method = 'OneHot'
colList = ['col2','col3']
'''


class CategoryColsEncodeApply_K(DService):

    def __init__(self):
        pass


    # 目标编码
    def Target_encoding_apply(self, target_enc, X_test, colList):
        #target_enc = ce.TargetEncoder(cols=colList).fit(X_train, X_train[labelCol])
        target_Df = target_enc.transform(X_test)
        colList_new = []
        for col in target_Df.columns.tolist():
            if col in colList:
                colList_new.append(col + '_Target')
            else:
                colList_new.append(col)
        target_Df.columns = colList_new
        return target_Df

    # WOE编码
    def WOE_encoding_apply(self, woe_enc, X_test, colList):
        #woe_enc = ce.WOEEncoder(cols=colList).fit(X_train, X_train[labelCol])
        woe_Df = woe_enc.transform(X_test)
        colList_new = []
        for col in woe_Df.columns.tolist():
            if col in colList:
                colList_new.append(col + '_WOE')
            else:
                colList_new.append(col)
        woe_Df.columns = colList_new
        return woe_Df



    def CategoryEncode(self, ClassDf,enc_rule_dict,method,colList,K):
        '''
        :param ClassDf: 待编码数据集
        :param enc_rule: 编码规则集
        :param method: 编码方式（one-hot编码：OneHot；序列编码：Ordinal；目标编码：Target；WOE编码：WOE）
        :param colList: 需要编码的列，list格式，如：['col2','col3']
        :param K: K折交叉验证
        :return: 应用编码后的数据集
        '''
        otherCols = list(set(ClassDf.columns.tolist()) - set(colList))
        colList_method = [col + '_' + method  for col in colList]
        ClassDf_All = ClassDf[otherCols]
        for i in range(K):
            enc_rule = enc_rule_dict[i]
            if method == 'Target':
                ClassDf_enc = self.Target_encoding_apply(enc_rule, ClassDf,colList)
            elif method == 'WOE':
                ClassDf_enc = self.WOE_encoding_apply(enc_rule, ClassDf,colList)

            colList_new_all = []
            for col in ClassDf_enc.columns.tolist():
                if col in colList_method:
                    colList_new_all.append(col + '_' + str(i))
                else:
                    colList_new_all.append(col)
            ClassDf_enc.columns = colList_new_all
            ClassDf_All = ClassDf_All.merge(ClassDf_enc,on=otherCols,how='left')

        ClassDf_All_new = ClassDf[otherCols]
        for cols in colList:
            temp_cols = []
            for cols_1 in ClassDf_All.columns.tolist():
                if cols in cols_1:
                    temp_cols.append(cols_1)
            ClassDf_All_new[cols + '_' + method] = ClassDf_All[temp_cols].mean(axis=1)

        return ClassDf_All_new


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
        enc_rule_dict = inputDf.get('inputDf2')

        # 控制参数
        mapping = dicParams['mapping']
        method = mapping.get('method')
        colList = mapping.get('colList')
        K = mapping.get('K')

        # 数据处理
        ClassDf_enc = self.CategoryEncode(ClassDf, enc_rule_dict, method, colList,K)
        # 输出
        #outputDf = inputDf
        dicOutput['outputDf1'] = ClassDf_enc

        return dicOutput