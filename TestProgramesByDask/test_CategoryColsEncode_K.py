from unittest import TestCase

import pandas as pd
from IServiceImp.CategoryColsEncode_K import CategoryColsEncode_K
from sklearn.externals import joblib


class TestCategoryColsEncode_K(TestCase):
    def test_process(self):
        dataDf = pd.read_csv("E:/PythonProject/tgtks/data/TrainData.csv")
        input = dict()
        input['inputDf'] = {'inputDf1': dataDf}


        params = dict()
        params['mapping'] = {'labelCol': 'label', 'method': 'WOE', 'colList': ['col2','col3'], 'K': 3}


        bs = CategoryColsEncode_K()
        outputDict = bs.process(input, params)
        outDf1 = outputDict['outputDf1']
        outDf2 = outputDict['outputDf2']
        outDf1.to_csv("E:/PythonProject/tgtks/testResut/enc_df.csv",index=None)
        #outDf2.to_csv("E:/PythonProject/tgtks/testResut/enc_rule.csv")
        joblib.dump(outDf2, 'E:/PythonProject/tgtks/testResut/woe_enc_1.m')
