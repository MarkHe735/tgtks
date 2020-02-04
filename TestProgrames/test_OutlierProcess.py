from unittest import TestCase

import pandas as pd
from IServiceImp.OutlierProcess import OutlierProcess


class TestOutlierProcess(TestCase):
    def test_process(self):
        dataDf = pd.read_csv(r"E:\PythonProject\tgtks\data\trainData.csv")
        profileDf = pd.read_csv(r"E:\PythonProject\tgtks\TestResData\ProfileResData.csv")
        input = dict()
        input['inputDf1'] = dataDf
        input['inputDf2'] = profileDf

        params = dict()
        params['mapping'] = {'colsMap': {'col1': 'IQR', 'col3': 'sigma'},
                             # 用于判断正态分布相似度:
                             'point': 15.0}

        bs = OutlierProcess()
        outputDict = bs.process(input, params)
        outDf = outputDict['outputDf']
        outDf.to_csv(r"E:\PythonProject\tgtks\TestResData\OutlierData.csv")
        pass
