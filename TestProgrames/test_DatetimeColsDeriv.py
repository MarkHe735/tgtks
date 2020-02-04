from unittest import TestCase

import pandas as pd
from IServiceImp.DatetimeColsDeriv import DatetimeColsDeriv
from datetime import datetime
dateparse = lambda dates: datetime.strptime(dates, '%Y/%m/%d')

class TestDatetimeColsDeriv(TestCase):
    def test_process(self):
        dataDf = pd.read_csv("/home/wj/code3/knimeModule/knime20191126/data/TrainData_datatime.csv", parse_dates = ['applyDate'],date_parser = dateparse)
        input = dict()
        input['inputDf'] = {'inputDf1': dataDf}


        params = dict()
        params['mapping'] = {'idCol': 'id'}


        bs = DatetimeColsDeriv()
        outputDict = bs.process(input, params)
        outDf1 = outputDict['outputDf1']
        outDf1.to_csv("/home/wj/code3/knimeModule/knime20191126/testResut/date_df.csv",index=None)
