from unittest import TestCase

import pandas as pd
from IServiceImp.DataMerge import DataMerge



class TestDataMerge(TestCase):
    def test_process(self):
        dataDf1 = pd.read_csv("/home/wj/code3/knimeModule/knime20191126/data/TrainData_datatime.csv")
        dataDf2 = pd.read_csv("/home/wj/code3/knimeModule/knime20191126/data/TestData_numeric.csv")
        dataDf3 = pd.read_csv("/home/wj/code3/knimeModule/knime20191126/data/TrainData_class.csv")
        labelDf = pd.read_csv("/home/wj/code3/knimeModule/knime20191126/data/TrainData_label.csv")
        input = dict()
        input['inputDf'] = {'inputDf1': dataDf1,'inputDf2':dataDf2,'inputDf3':dataDf3,'inputDf4':labelDf}

        params = dict()
        params['mapping'] = {'idCol': 'id'}

        bs = DataMerge()
        outputDict = bs.process(input, params)
        outDf1 = outputDict['outputDf1']
        outDf1.to_csv("/home/wj/code3/knimeModule/knime20191126/testResut/data_merge.csv",index=None)
