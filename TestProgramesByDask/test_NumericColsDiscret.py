from unittest import TestCase

import pandas as pd
from IServiceImp.NumericColsDiscret import NumericColsDiscret
from sklearn.externals import joblib


class TestNumericColsDiscret(TestCase):
    def test_process(self):
        dataDf = pd.read_csv("/home/wj/code3/knimeModule/knime20191126/data/TrainData_numeric.csv")
        input = dict()
        input['inputDf'] = {'inputDf1': dataDf}


        params = dict()
        params['mapping'] = {'idCol': 'id', 'method': 'EqualWidth', 'colList': ['value1','value2'],'binningNum':3}


        bs = NumericColsDiscret()
        outputDict = bs.process(input, params)
        outDf1 = outputDict['outputDf1']
        outDf2 = outputDict['outputDf2']
        outDf1.to_csv("/home/wj/code3/knimeModule/knime20191126/testResut/bin_df.csv",index=None)
        #outDf2.to_csv("D:\\PythonDev\\Code\\TriGoldenToolKits\\data\\out2.csv")
        joblib.dump(outDf2, '/home/wj/code3/knimeModule/knime20191126/testResut/bin_EqualWidth.m')