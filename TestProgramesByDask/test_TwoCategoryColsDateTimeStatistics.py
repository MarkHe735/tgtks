from unittest import TestCase

import pandas as pd
from IServiceImp.TwoCategoryColsDateTimeStatistics import TwoCategoryColsDateTimeStatistics


class TestTwoCategoryColsDateTimeStatistics(TestCase):
    def test_process(self):
        dataDf = pd.read_csv("E:/PythonProject/tgtks/data/TrainDataForFeaturetools.csv")
        input = dict()
        input['inputDf'] = dataDf

        params = dict()
        params['mapping'] = {'category_col1': 'sex', 'category_col2': 'label', 'id_col': 'id',
                             'datetime_col': 'applyDate',
                             'statistics_methods': ["mean", "max", "min", "count", "sum", "std"]}

        bs = TwoCategoryColsDateTimeStatistics()
        outputDict = bs.process(input, params)
        outDf = outputDict['outputDf']
        outDf.to_csv("E:/PythonProject/tgtks/testResut/TwoCategoryColsDateTimeStatistics.csv")
