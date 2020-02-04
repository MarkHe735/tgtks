from unittest import TestCase

import pandas as pd
from IServiceImp.CategoryColsEncodeApply_K import CategoryColsEncodeApply_K
from sklearn.externals import joblib


class TestCategoryColsEncodeApply_K(TestCase):
    def test_process(self):
        dataDf = pd.read_csv("/home/wj/code3/knimeModule/knime20191126/data/TestData.csv")
        #ruleDf = pd.read_csv("/home/wj/code3/knimeModule/knime20191126/data/TrainData_merge_rule.csv")
        ruleDf = joblib.load("/home/wj/code3/knimeModule/knime20191126/testResut/woe_enc_1.m")
        input = dict()
        input['inputDf'] = {'inputDf1':dataDf,'inputDf2':ruleDf}



        params = dict()
        params['mapping'] = {'method': 'WOE', 'colList': ['col2','col3'], 'K': 3}



        bs = CategoryColsEncodeApply_K()
        outputDict = bs.process(input,params)
        outDf1 = outputDict['outputDf1']
        outDf1.to_csv("/home/wj/code3/knimeModule/knime20191126/testResut/enc_df_apply_1.csv",index=None)
