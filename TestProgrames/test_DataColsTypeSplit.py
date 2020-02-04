from unittest import TestCase

import pandas as pd
from IServiceImp.DataColsTypeSplit import DataColsTypeSplit
from datetime import datetime

dateparse = lambda dates: datetime.strptime(dates, '%Y/%m/%d')

class TestDataColsTypeSplit(TestCase):
    def test_process(self):
        dataDf = pd.read_csv("/home/wj/code3/knimeModule/knime20191126/data/TrainData.csv", parse_dates=['applyDate'],
                      date_parser=dateparse,
                      dtype={'id': int, 'col1': object, 'col2': object, 'col3': object, 'value1': int, 'value2': int,
                             'label': int})
        input = dict()
        input['inputDf'] = {'inputDf1': dataDf}


        params = dict()
        params['mapping'] = {'idCol': 'id', 'labelCol': 'label'}


        bs = DataColsTypeSplit()
        outputDict = bs.process(input,params)
        outDf1 = outputDict['outputDf1']
        outDf2 = outputDict['outputDf2']
        outDf3 = outputDict['outputDf3']
        labelDf = outputDict['outputDf4']
        outDf1.to_csv("/home/wj/code3/knimeModule/knime20191126/testResut/DateDf.csv",index=None)
        outDf2.to_csv("/home/wj/code3/knimeModule/knime20191126/testResut/ValueDf.csv",index=None)
        outDf3.to_csv("/home/wj/code3/knimeModule/knime20191126/testResut/ClassDf.csv",index=None)
        labelDf.to_csv("/home/wj/code3/knimeModule/knime20191126/testResut/LabelDf.csv",index=None)
