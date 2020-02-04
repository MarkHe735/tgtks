from unittest import TestCase

import pandas as pd
from IServiceImp.TrainTestSplitter import TrainTestSplitter


class TestTrainTestSplitter(TestCase):
    def test_process(self):
        dataDf = pd.read_csv(r'E:\PythonProject\tgtks\TestData\RawData.csv')
        input = dict()
        input['inputDf'] = dataDf

        params = dict()
        params['mapping'] = {'labelCol': 'label', 'testRatio': 0.3}

        bs = TrainTestSplitter()
        outputDict = bs.process(input, params)
        train_df = outputDict['outputDf1']
        test_df = outputDict['outputDf2']
        train_df.to_csv(r"E:\PythonProject\tgtks\TestResData\splittedTrainSet.csv")
        test_df.to_csv(r"E:\PythonProject\tgtks\TestResData\splittedTestSet.csv")
