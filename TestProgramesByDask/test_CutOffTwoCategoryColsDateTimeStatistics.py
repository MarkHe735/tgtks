from unittest import TestCase

import pandas as pd
from IServiceImp.CutOffTwoCategoryColsDateTimeStatistics import CutOffTwoCategoryColsDateTimeStatistics


class TestCutOffTwoCategoryColsDateTimeStatistics(TestCase):
    def test_process(self):
        dataDf = pd.read_csv(r"G:\XinYongKa.csv")
        dataDf['id'] = range(len(dataDf))
        input = dict()
        input['inputDf'] = dataDf

        params = dict()
        # params['mapping'] = {'category_col': 'CCY_CD', 'id_col': 'id', 'datetime_col': 'BGN_DT',
        #                      'statistics_methods': ["mean", "max", "min", "count", "sum", "std"],
        #                      'cutoff_time': '2014/3/31', 'time_window': '3 month'}

        # params['mapping'] = {'category_col1': 'CCY_CD', 'category_col2': 'FIVE_CGY_CD', 'id_col': 'id',
        #                      'datetime_col': 'BGN_DT',
        #                      'statistics_methods': ["mean", "max", "min", "count", "sum", "std"],
        #                      'cutoff_time': '2014/3/31', 'time_window': '3 month'}
        params['mapping'] = {'category_col1': 'CC_VLD_IND', 'category_col2': 'PD_TP_CD', 'id_col': 'id',
                             'datetime_col': 'dt',
                             'statistics_methods': ["mean", "max", "min", "count", "sum", "std"],
                             'cutoff_time': '2018/5/28', 'time_window': '6 month'}

        bs = CutOffTwoCategoryColsDateTimeStatistics()
        outputDict = bs.process(input, params)
        outDf = outputDict['outputDf']
        outDf.to_csv("E:/PythonProject/tgtks/testResut/CutOffTwoCategoryColsDateTimeStatistics.csv")
