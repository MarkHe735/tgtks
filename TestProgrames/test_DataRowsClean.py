from unittest import TestCase

import pandas as pd
from IServiceImp.DataRowsClean import DataRowsClean


class TestDataRowsClean(TestCase):
    def test_process(self):
        dataDf = pd.read_csv(r"E:\PythonProject\tgtks\TestData\TrainData.csv")
        input = dict()
        input['inputDf'] = dataDf

        params = dict()
        params['mapping'] = {'MissRate': 0.126}

        bs = DataRowsClean()
        outputDict = bs.process(input, params)
        outDf = outputDict['outputDf']
        outDf.to_csv(r"E:\PythonProject\tgtks\TestResData\RowCleanData.csv")
