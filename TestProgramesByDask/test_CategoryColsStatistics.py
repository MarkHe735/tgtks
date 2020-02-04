from unittest import TestCase

import pandas as pd
from IServiceImp.CategoryColsStatistics import CategoryColsStatistics


class TestCategoryColsStatistics(TestCase):
    def test_process(self):
        dataDf = pd.read_csv("E:/PythonProject/tgtks/data/TrainDataForFeaturetools.csv")
        input = dict()
        input['inputDf'] = dataDf

        params = dict()
        params['mapping'] = {'category_col': 'sex', 'num_cols': ['col1', 'value2'], 'id_col': 'id',
                             'statistics_methods': ["mean", "max", "min", "count", "sum", "std"]}

        bs = CategoryColsStatistics()
        outputDict = bs.process(input, params)
        outDf = outputDict['outputDf']
        outDf.to_csv("E:/PythonProject/tgtks/testResut/CategoryColsStatistics.csv")
