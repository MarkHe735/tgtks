from unittest import TestCase

import pandas as pd
from IServiceImp.TwoDatetimeSubtract import TwoDatetimeSubtract


class TestTwoDatetimeSubtract(TestCase):
    def test_process(self):
        dataDf = pd.read_csv(r"E:\PythonProject\tgtks\data\DateDf_jinling.csv")
        input = dict()
        input['inputDf'] = dataDf

        params = dict()
        # key_datetime - value_datetime, like {'datetime1': 'datetime2'} means 'datetime1'-'datetime2'
        params['mapping'] = {'datetime1': 'datetime2', 'datetime3': 'datetime4'}

        bs = TwoDatetimeSubtract()
        outputDict = bs.process(input, params)
        outDf = outputDict['outputDf']
        outDf.to_csv("E:/PythonProject/tgtks/testResut/TwoDatetimeSubtract.csv")
