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

'''
labelCol = 'label'

ClassDf = pd.read_csv('/home/wj/code3/knimeModule/knime20191126/data/TrainData.csv')
#joblib.dump(target_enc, '/home/wj/code3/knimeModule/knime20191126/data/target_enc.m')
target_enc_1 = joblib.load("/home/wj/code3/knimeModule/knime20191126/data/target_enc.m")


#one-hot编码：OneHot；序列编码：Ordinal；目标编码：Target；WOE编码：WOE
method = 'OneHot'
colList = ['col2','col3']
'''


class CategoryColsEncodeApply(DService):

    def __init__(self):
        pass


    # one-hot编码
    def OneHot_encoding_apply(self, onehot_enc,X_train,idCol, colList):
        X_train = X_train[colList + [idCol]]
        #onehot_enc = ce.OneHotEncoder(cols=colList).fit(X_train)
        onehot_Df = onehot_enc.transform(X_train)
        return onehot_Df

    # 序列编码
    def Ordinal_encoding_apply(self,ordinal_enc, X_train,idCol, colList):
        X_train = X_train[colList + [idCol]]
        #ordinal_enc = ce.OrdinalEncoder(cols=colList).fit(X_train)
        ordinal_Df = ordinal_enc.transform(X_train)
        colList_new = []
        for col in ordinal_Df.columns.tolist():
            if col in colList:
                colList_new.append(col + '_Ordinal')
            else:
                colList_new.append(col)
        ordinal_Df.columns = colList_new
        return ordinal_Df

    # 计数编码
    def Count_encoding_apply(self,Count_enc, X_train,idCol, colList):
        X_train = X_train[colList + [idCol]]
        #Count_enc = X_train[colList].apply(pd.value_counts).fillna(0)
        #Count_enc['ClassGroup'] = Count_enc.index
        #Count_enc = Count_enc.reset_index(drop=True)

        for cols in colList:
            X_train = X_train.merge(Count_enc[[cols + '_Count','ClassGroup']],left_on = cols ,right_on = 'ClassGroup',how='left' )
            X_train.drop(columns = ['ClassGroup'],inplace = True)
        X_train.drop(columns = colList,inplace = True)
        return X_train

    # 频率编码
    def Frequency_encoding_apply(self, Frequency_enc,X_train,idCol, colList):
        X_train = X_train[colList + [idCol]]
        #Frequency_enc = X_train[colList].apply(lambda x:x.value_counts(normalize=True,dropna=False))

        for cols in colList:
            X_train = X_train.merge(Frequency_enc[[cols + '_Frequency', 'ClassGroup']], left_on=cols, right_on='ClassGroup',
                                    how='left')
            X_train.drop(columns=['ClassGroup'], inplace=True)
        X_train.drop(columns=colList, inplace=True)
        return X_train



    #CatBoost编码
    def CatBoost_encoding_apply(self,CatBoost_enc, X_train, idCol,labelCol, colList):
        X_train = X_train[colList + [idCol,labelCol]]
        #CatBoost_enc = ce.CatBoostEncoder(cols=colList).fit(X_train, X_train[labelCol])
        CatBoost_Df = CatBoost_enc.transform(X_train)
        colList_new = []
        for col in CatBoost_Df.columns.tolist():
            if col in colList:
                colList_new.append(col + '_CatBoost')
            else:
                colList_new.append(col)
        CatBoost_Df.columns = colList_new
        CatBoost_Df.drop(columns=labelCol, inplace=True)
        return CatBoost_Df


    # 目标编码
    def Target_encoding_apply(self, target_enc,X_train, idCol,labelCol, colList):
        X_train = X_train[colList + [idCol, labelCol]]
        #target_enc = ce.TargetEncoder(cols=colList).fit(X_train, X_train[labelCol])
        target_Df = target_enc.transform(X_train)
        colList_new = []
        for col in target_Df.columns.tolist():
            if col in colList:
                colList_new.append(col + '_Target')
            else:
                colList_new.append(col)
        target_Df.columns = colList_new
        target_Df.drop(columns=labelCol, inplace=True)
        return target_Df

    # WOE编码
    def WOE_encoding_apply(self,woe_enc, X_train, idCol,labelCol, colList):
        X_train = X_train[colList + [idCol, labelCol]]
        #woe_enc = ce.WOEEncoder(cols=colList).fit(X_train, X_train[labelCol])
        woe_Df = woe_enc.transform(X_train)
        colList_new = []
        for col in woe_Df.columns.tolist():
            if col in colList:
                colList_new.append(col + '_WOE')
            else:
                colList_new.append(col)
        woe_Df.columns = colList_new
        woe_Df.drop(columns=labelCol, inplace=True)
        return woe_Df



    def CategoryEncode(self, ClassDf, enc_rule,idCol,labelCol,method_cols_dict):
        '''
        :param ClassDf: 待编码数据集
        :param enc_rule: 编码规则集
        :param idCol: id列
        :param labelCol: label标签名
        :param method_cols_dict: {编码方式1：需要分箱的列1；编码方式2：需要分箱的列2} 【编码方式（one-hot编码：OneHot；序列编码：Ordinal；计数编码:Count;频率编码:Frequency;CatBoost编码：CatBoost；目标编码：Target；WOE编码：WOE）；需要分箱的列，list格式，如：['col2','col3']】，如果所有变量用同一种编码方式，可以写{'Frequency':'all'}
        :return: 应用编码后的数据集
        '''
        # 保留不需要编码的列
        enc_list_all = []
        for method in method_cols_dict.keys():
            if method_cols_dict[method] == 'all':
                enc_list_all = list(set(ClassDf.columns.tolist()).difference(set([idCol, labelCol])))
            else:
                enc_list_all = enc_list_all + method_cols_dict[method]
        no_enc_list_all = list(set(ClassDf.columns.tolist()).difference(set(enc_list_all)))  # 差集
        ClassDf_enc_All = ClassDf[no_enc_list_all]


        for method in method_cols_dict.keys():
            if method == 'OneHot':
                ClassDf_enc = self.OneHot_encoding_apply(enc_rule[method],ClassDf, idCol, method_cols_dict[method])
            elif method == 'Ordinal':
                ClassDf_enc = self.Ordinal_encoding_apply(enc_rule[method],ClassDf, idCol, method_cols_dict[method])
            elif method == 'Count':
                ClassDf_enc = self.Count_encoding_apply(enc_rule[method],ClassDf, idCol, method_cols_dict[method])
            elif method == 'Frequency':
                ClassDf_enc = self.Frequency_encoding_apply(enc_rule[method],ClassDf, idCol, method_cols_dict[method])
            elif method == 'CatBoost':
                ClassDf_enc = self.CatBoost_encoding_apply(enc_rule[method],ClassDf, idCol, labelCol, method_cols_dict[method])
            elif method == 'Target':
                ClassDf_enc = self.Target_encoding_apply(enc_rule[method],ClassDf, idCol, labelCol, method_cols_dict[method])
            elif method == 'WOE':
                ClassDf_enc = self.WOE_encoding_apply(enc_rule[method],ClassDf, idCol, labelCol, method_cols_dict[method])
            ClassDf_enc_All = ClassDf_enc_All.merge(ClassDf_enc, on=idCol, how='left')
        return ClassDf_enc_All



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
        ClassDf = dicInput['inputDf1']
        enc_rule = dicInput['inputDf2']

        # 控制参数
        mapping = dicParams['mapping']
        idCol = mapping.get('idCol')
        labelCol = mapping.get('labelCol')
        method_cols_dict = mapping.get('method_cols_dict')

        # 数据处理
        ClassDf_enc = self.CategoryEncode(ClassDf, enc_rule,idCol,labelCol,method_cols_dict)
        # 输出
        #outputDf = inputDf
        dicOutput['outputDf1'] = ClassDf_enc

        return dicOutput