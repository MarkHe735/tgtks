from unittest import TestCase

import pandas as pd
from IServiceImp.CategoryColsEncodeApply import CategoryColsEncodeApply
from sklearn.externals import joblib


class TestCategoryColsEncodeApply(TestCase):
    def test_process(self):
        dataDf = pd.read_csv("/home/wj/code3/knimeModule/knime20191126/data/TrainData.csv")
        #ruleDf = pd.read_csv("/home/wj/code3/knimeModule/knime20191126/data/TrainData_merge_rule.csv")
        ruleDf = joblib.load("/home/wj/code3/knimeModule/knime20191126/testResut/rule_enc_all.m")
        input = dict()
        input['inputDf1'] = dataDf
        input['inputDf2'] = ruleDf



        params = dict()
        params['mapping'] = {'idCol': 'id','labelCol': 'label',  'method_cols_dict' : {'Count':['col1'],'CatBoost':['value1','col2']}}



        bs = CategoryColsEncodeApply()
        outputDict = bs.process(input,params)
        outDf1 = outputDict['outputDf1']
        outDf1.to_csv("/home/wj/code3/knimeModule/knime20191126/testResut/enc_df_apply_train_1.csv",index=None)
