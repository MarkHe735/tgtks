from unittest import TestCase

import pandas as pd
from IServiceImp.DataProfile import DataProfile


class TestDataProfile(TestCase):
    def test_process(self):
        dataDf = pd.read_csv(r"E:\PythonProject\tgtks\TestData\TrainData.csv")
        input = dict()
        input['inputDf'] = dataDf

        # useless parameter, to unify the method invoking.
        params = dict()
        params['mapping'] = {'col1': 'mean', 'col2': 'median', 'col3': 'mode'}

        bs = DataProfile()
        outputDict = bs.process(input, params)
        outDf1 = outputDict['outputDf']
        outDf1.to_csv(r"E:\PythonProject\tgtks\TestResData\ProfileResData.csv")

