from unittest import TestCase

import pandas as pd
from IServiceImp.DataColsClean import DataColsClean


class TestDataColsClean(TestCase):
    def test_process(self):
        dataDf = pd.read_csv(r"E:\PythonProject\tgtks\data\TrainData.csv")
        profileDf = pd.read_csv(r"E:\PythonProject\tgtks\TestResData\ProfileResData.csv")
        input = dict()
        input['inputDf1'] = dataDf
        input['inputDf2'] = profileDf

        params = dict()
        control_paras = {'keepCols': ['id', 'applyDate', 'label'],
                         'missThreshold': 0.4, 'uniqueThreshold': 0.9, 'constantThreshold': 0.8,
                         'missThresholdDict': {'col1': 0.7, 'col2': 0.6}}
        params['mapping'] = control_paras

        bs = DataColsClean()
        outputDict = bs.process(input, params)
        outDf = outputDict['outputDf']
        outDf.to_csv(r"E:\PythonProject\tgtks\TestResData\DelColumnsData.csv")
