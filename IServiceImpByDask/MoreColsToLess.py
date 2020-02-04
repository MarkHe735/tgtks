#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : MoreColsToLess.py
@ Author: WangJun
@ Date  : 2019-12-05
@ Desc  : 多列变少列：按照保留变量个数，保留数据

"""
import pandas as pd
import numpy as np
from IService.DService import DService
from sklearn.feature_selection import mutual_info_classif, chi2
from sklearn.feature_selection import SelectKBest, SelectPercentile
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import roc_auc_score
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split


'''
from sklearn.datasets import load_breast_cancer
data = load_breast_cancer()
data = pd.DataFrame(np.c_[data['data'], data['target']],
                  columns= np.append(data['feature_names'], ['target']))
'''
#自变量间相关系数:Pearson; 自变量与Y相关系数:Kendall; 互信息（联合分布与边缘分布的相对熵）：Entropy；卡方：ChiSquare; 树模型相关性：Tree; GBDT特征重要性：GBDT

class MoreColsToLess(DService):

    def __init__(self):
        pass

    def Compute_Corr_X(self,Binning_Train_Result, idCol, labelCol,select_k):
        BinningColAll = Binning_Train_Result.columns.tolist()
        BinningColAll.remove(idCol)
        BinningColAll.remove(labelCol)
        Binning_Train_X = Binning_Train_Result.loc[:, BinningColAll]
        # 变量间的相关性("pearson-皮尔逊"相关系数:(ρX,Y)等于它们之间的协方差cov(X,Y)除以它们各自标准差的乘积(σX, σY))
        X_corr_X = Binning_Train_X.corr()
        X_corr_X_col = X_corr_X.columns.tolist()
        # X_corr_X_L = np.tril(X_corr_X, -1)     #转成下三角矩阵
        X_corr_X_U = np.triu(X_corr_X, 1)  # 转成上三角矩阵
        X_corr_X_L_Df = pd.DataFrame(X_corr_X_U, columns=X_corr_X_col, index=X_corr_X_col)
        X_corr_X_1 = X_corr_X_L_Df.stack()  # 将数据的列“旋转”为行，转成n*n行，3列的矩阵（unstack：将数据的行“旋转”为列）
        col1 = [X_corr_X_1.index[i][0] for i in range(len(X_corr_X_1.index))]
        col2 = [X_corr_X_1.index[j][1] for j in range(len(X_corr_X_1.index))]
        X_corr_X_2 = pd.DataFrame({'col1': col1, 'col2': col2, 'corr': X_corr_X_1.values})
        X_corr_X_3 = X_corr_X_2.loc[X_corr_X_2.loc[:, 'corr'] != 0, :]
        X_corr_X_3['corr_abs'] = X_corr_X_3['corr'].abs()
        X_corr_X_3.sort_values(by='corr_abs',ascending=True,inplace=True)
        X_corr_X_3 = X_corr_X_3.reset_index(drop=True)

        if select_k >= 1 :
            num = select_k
        elif 0 < select_k < 1:
            num = int(np.ceil(select_k * Binning_Train_Result.shape[1]))
        else:
            raise ValueError("select_k must be a positive number")

        i = 0
        n = 0
        while n <= num:
            colsDf1 = X_corr_X_3.loc[0:i,'col1']
            colsDf2 = X_corr_X_3.loc[0:i,'col2']
            cols = list(set(colsDf1.tolist() + colsDf2.tolist()))
            n = len(cols)
            i = i+1

        if n > num:
            col = cols[0:num]
        else:
            col = cols
        return col


    def Compute_Corr_Y(self, Binning_Train_Result, idCol, labelCol,select_k):
        BinningColAll = Binning_Train_Result.columns.tolist()
        BinningColAll.remove(idCol)
        Binning_Train_X = Binning_Train_Result.loc[:, BinningColAll]
        # 自变量与因变量相关性（肯德尔）
        X_corr_Y = Binning_Train_X.corr('kendall')
        X_corr_Y_filter = X_corr_Y[labelCol]
        X_corr_Y_T = pd.DataFrame({'col': X_corr_Y_filter.index, 'value': X_corr_Y_filter})
        X_corr_Y_T['value_abs'] = X_corr_Y_T['value'].abs()
        X_corr_Y_T.sort_values(by='value_abs',ascending=False,inplace=True)
        X_corr_Y_T = X_corr_Y_T.reset_index(drop=True)

        if select_k >= 1 :
            cols = X_corr_Y_T.loc[1:select_k,'col']
            col = cols.tolist()
        elif 0 < select_k < 1:
            num = np.ceil(select_k * Binning_Train_Result.shape[1])
            cols = X_corr_Y_T.loc[1:num,'col']
            col = cols.tolist()
        else:
            raise ValueError("select_k must be a positive number")
        return col


    def mutual_info(self,X, y, select_k=10):
        #    mi = mutual_info_classif(X,y)
        #    mi = pd.Series(mi)
        #    mi.index = X.columns
        #    mi.sort_values(ascending=False)

        if select_k >= 1:
            sel_ = SelectKBest(mutual_info_classif, k=select_k).fit(X, y)
            col = X.columns[sel_.get_support()]

        elif 0 < select_k < 1:
            sel_ = SelectPercentile(mutual_info_classif, percentile=select_k * 100).fit(X, y)
            col = X.columns[sel_.get_support()]

        else:
            raise ValueError("select_k must be a positive number")

        return col


    def chi_square_test(self,X, y, select_k=10):
        """
        Compute chi-squared stats between each non-negative feature and class.
        This score should be used to evaluate categorical variables in a classification task
        """
        if select_k >= 1:
            sel_ = SelectKBest(chi2, k=select_k).fit(X, y)
            col = X.columns[sel_.get_support()]
        elif 0 < select_k < 1:
            sel_ = SelectPercentile(chi2, percentile=select_k * 100).fit(X, y)
            col = X.columns[sel_.get_support()]
        else:
            raise ValueError("select_k must be a positive number")

        return col


    def univariate_roc_auc(self,X_train, y_train, X_test, y_test, select_k):
        """
        First, it builds one decision tree per feature, to predict the target
        Second, it makes predictions using the decision tree and the mentioned feature
        Third, it ranks the features according to the machine learning metric (roc-auc or mse)
        It selects the highest ranked features
        """
        roc_values = []
        for feature in X_train.columns:
            clf = DecisionTreeClassifier()
            clf.fit(X_train[feature].to_frame(), y_train)
            y_scored = clf.predict_proba(X_test[feature].to_frame())
            roc_values.append(roc_auc_score(y_test, y_scored[:, 1]))
        roc_values_Df = pd.DataFrame({'Cols': list(X_train.columns), 'AUC': roc_values})
        roc_values_Df.sort_values(by='AUC', ascending=False, inplace=True)
        roc_values_Df = roc_values_Df.reset_index(drop=True)

        if select_k >= 1 :
            cols = roc_values_Df.loc[0:select_k - 1, 'Cols']
            col = cols.tolist()
        elif 0 < select_k < 1:
            num = np.ceil(select_k * len(roc_values_Df))
            cols = roc_values_Df.loc[0:num - 1, 'Cols']
            col = cols.tolist()
        else:
            raise ValueError("select_k must be a positive number")
        return col



    def gbt_importance(self,X_train, y_train,select_k, max_depth=10, n_estimators=50, random_state=0):
        model = GradientBoostingClassifier(n_estimators=n_estimators, max_depth=max_depth,
                                           random_state=random_state)
        model.fit(X_train, y_train)
        importances = model.feature_importances_
        importances_Df = pd.DataFrame({'Feature': list(X_train.columns), 'Importance': importances.tolist()})
        importances_Df.sort_values(by='Importance', ascending=False, inplace=True)
        importances_Df = importances_Df.reset_index(drop=True)

        if select_k >= 1 :
            cols = importances_Df.loc[0:select_k - 1,'Feature']
            col = cols.tolist()
        elif 0 < select_k < 1:
            num = np.ceil(select_k * len(importances_Df))
            cols = importances_Df.loc[0:num-1,'Feature']
            col = cols.tolist()
        else:
            raise ValueError("select_k must be a positive number")
        return col



    def MoreColsToLessMain(self,DataDf,idCol, labelCol,K, method):
        '''
        :param DataDf: 多列的数据集
        :param idCol: id列
        :param labelCol: label列
        :param K: 最终保留变量个数或占比
        :param method: 与标签无关的评价：《#自变量间相关系数:Pearson; 自变量与Y相关系数:Kendall;》；与标签有关的评价：《 互信息（联合分布与边缘分布的相对熵）：Entropy；卡方：ChiSquare; 树模型相关性：Tree; GBDT特征重要性：GBDT》
        :return: 列数变少的数据集
        '''
        ColAll = DataDf.columns.tolist()
        ColAll.remove(idCol)
        ColAll.remove(labelCol)
        X_train, X_test, y_train, y_test = train_test_split(DataDf[ColAll],DataDf[labelCol],test_size=0.3,random_state=0)

        if method == 'Pearson':
            keepCols = self.Compute_Corr_X(DataDf, idCol, labelCol,K)
        elif method == 'Kendall':
            keepCols = self.Compute_Corr_Y(DataDf, idCol, labelCol,K)
        elif method == 'Entropy':
            mi = self.mutual_info(X=DataDf[ColAll], y=DataDf[labelCol], select_k=K)
            keepCols = mi.tolist()
        elif method == 'ChiSquare':
            chi = self.chi_square_test(X=DataDf[ColAll], y=DataDf[labelCol], select_k=K)
            keepCols = chi.tolist()
        elif method == 'Tree':
            keepCols = self.univariate_roc_auc(X_train, y_train, X_test, y_test, K)
        elif method == 'GBDT':
            keepCols = self.gbt_importance(X_train=DataDf[ColAll], y_train=DataDf[labelCol],select_k = K)

        keepCols.append(idCol)
        keepCols.append(labelCol)
        DataDfLess = DataDf[keepCols]
        return DataDfLess


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
        idCol = mapping.get('idCol')
        labelCol = mapping.get('labelCol')
        K = mapping.get('K')
        method = mapping.get('method')


        # 数据处理
        DataDfLess = self.MoreColsToLessMain(inputDf, idCol, labelCol, K, method)

        # 输出
        #outputDf = inputDf
        dicOutput['outputDf'] = DataDfLess

        return dicOutput