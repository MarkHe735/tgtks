from unittest import TestCase

import pandas as pd
from IServiceImp.NumericColsDiscretApply import NumericColsDiscretApply
from sklearn.externals import joblib


class TestNumericColsDiscretApply(TestCase):
    def test_process(self):
        dataDf = pd.read_csv("/home/wj/code3/knimeModule/knime20191126/data/TestData_numeric.csv")
        #ruleDf = pd.read_csv("/home/wj/code3/knimeModule/knime20191126/data/TrainData_numeric_rule.csv")
        ruleDf = joblib.load("/home/wj/code3/knimeModule/knime20191126/testResut/bin_EqualWidth.m")

        input = dict()
        input['inputDf'] = {'inputDf1': dataDf, 'inputDf2': ruleDf}


        params = dict()
        params['mapping'] = {'idCol': 'id','method': 'EqualWidth', 'colList': ['value1','value2']}


        bs = NumericColsDiscretApply()
        outputDict = bs.process(input,params)
        outDf1 = outputDict['outputDf1']
        outDf1.to_csv("/home/wj/code3/knimeModule/knime20191126/testResut/bin_df_apply.csv",index=None)

