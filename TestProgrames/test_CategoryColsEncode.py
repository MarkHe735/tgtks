from unittest import TestCase

import pandas as pd
from IServiceImp.CategoryColsEncode import CategoryColsEncode
from sklearn.externals import joblib


class TestCategoryColsEncode(TestCase):
    def test_process(self):
        dataDf = pd.read_csv("/home/wj/code3/knimeModule/knime20191126/data/TrainData.csv")
        input = dict()
        input['inputDf'] = {'inputDf1': dataDf}


        params = dict()
        params['mapping'] = {'idCol': 'id','labelCol': 'label',  'method_cols_dict' : {'Count':['col1'],'CatBoost':['value1','col2']}}


        bs = CategoryColsEncode()
        outputDict = bs.process(input, params)
        outDf1 = outputDict['outputDf1']
        outDf2 = outputDict['outputDf2']
        outDf1.to_csv("/home/wj/code3/knimeModule/knime20191126/testResut/enc_df_all.csv",index=None)
        #outDf2.to_csv("/home/wj/code3/knimeModule/knime20191126/testResut/enc_rule.csv")
        joblib.dump(outDf2, '/home/wj/code3/knimeModule/knime20191126/testResut/rule_enc_all.m')
