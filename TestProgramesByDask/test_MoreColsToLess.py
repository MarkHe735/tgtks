from unittest import TestCase

import pandas as pd
from IServiceImp.MoreColsToLess import MoreColsToLess


class TestMoreColsToLess(TestCase):
    def test_process(self):
        dataDf = pd.read_csv("E:/PythonProject/tgtks/data/TrainData_notime.csv")
        input = dict()
        input['inputDf'] = dataDf

        params = dict()
        params['mapping'] = {'idCol': 'id', 'labelCol': 'label', 'K': 0.3, 'method': 'GBDT'}

        bs = MoreColsToLess()
        outputDict = bs.process(input, params)
        outDf = outputDict['outputDf']
        outDf.to_csv("E:/PythonProject/tgtks/testResut/Less_df.csv", index=None)