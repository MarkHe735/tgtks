from unittest import TestCase

import pandas as pd
from IServiceImp.CutOffCategoryColsDateTimeStatistics import CutOffCategoryColsDateTimeStatistics


class TestCutOffCategoryColsDateTimeStatistics(TestCase):
    def test_process(self):
        dataDf = pd.read_csv(r"G:\FILE_STORAGE\06-Data_user\泰隆客户流失\DaiKuan.csv", nrows=1040000)
        dataDf['id'] = range(len(dataDf))
        input = dict()
        input['inputDf'] = dataDf

        params = dict()
        params['mapping'] = {'category_col': 'CCY_CD', 'id_col': 'id', 'datetime_col': 'BGN_DT',
                             'statistics_methods': ["mean", "max", "min", "count", "sum", "std"],
                             'cutoff_time': '2014/3/31', 'time_window': '3 month'}

        bs = CutOffCategoryColsDateTimeStatistics()
        outputDict = bs.process(input, params)
        outDf = outputDict['outputDf']
        outDf.to_csv("E:/PythonProject/tgtks/testResut/CutOffCategoryColsDateTimeStatistics.csv")
