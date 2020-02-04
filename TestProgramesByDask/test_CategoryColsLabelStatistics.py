from unittest import TestCase

import pandas as pd
from IServiceImp.CategoryColsLabelStatistics import CategoryColsLabelStatistics


class TestCategoryColsLabelStatistics(TestCase):
    def test_process(self):
        dataDf = pd.read_csv("E:/PythonProject/tgtks/data/TrainDataForFeaturetools.csv")
        input = dict()
        input['inputDf'] = dataDf

        params = dict()
        params['mapping'] = {'category_col': 'sex', 'num_cols': ['col1', 'value2'], 'id_col': 'id',
                             'label_col': 'label',
                             'statistics_methods': ["mean", "max", "min", "count", "sum", "std"]}

        bs = CategoryColsLabelStatistics()
        outputDict = bs.process(input, params)
        outDf = outputDict['outputDf']
        outDf.to_csv("E:/PythonProject/tgtks/testResut/CategoryColsLabelStatistics.csv")
