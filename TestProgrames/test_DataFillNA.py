from unittest import TestCase

import pandas as pd
from IServiceImp.DataFillNA import DataFillNA


class TestDataFileNA(TestCase):
    def test_process(self):
        dataDf = pd.read_csv(r"E:\PythonProject\tgtks\data\trainData.csv")
        profileDf = pd.read_csv(r"E:\PythonProject\tgtks\TestResData\ProfileResData.csv")
        input = dict()
        input['inputDf1'] = dataDf
        input['inputDf2'] = profileDf

        params = dict()
        params['mapping'] = {'defaultFill': {'num': 'Mean', 'enum': 'Mode', 'datetime': 'bfill'},
                             'colsMap': {'col1': 'Max', 'col2': 'CONST_0', 'col3': 'CONST_1'}}

        bs = DataFillNA()
        outputDict = bs.process(input, params)
        outDf = outputDict['outputDf']
        outDf.to_csv(r"E:\PythonProject\tgtks\TestResData\FillNaData.csv")
