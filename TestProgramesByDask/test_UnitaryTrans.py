from unittest import TestCase

import pandas as pd
from IServiceImp.UnitaryTrans import UnitaryTrans


class TestUnitaryTrans(TestCase):
    def test_process(self):
        dataDf = pd.read_csv("E:/PythonProject/tgtks/data/TrainData.csv")
        input = dict()
        input['inputDf'] = dataDf

        params = dict()
        params['mapping'] = {'col1': 'ln', 'col3': 'ln(1+p)', 'value1': 'square', 'value2': 'sqrt'}

        bs = UnitaryTrans()
        outputDict = bs.process(input, params)
        outDf = outputDict['outputDf']
        outDf.to_csv("E:/PythonProject/tgtks/testResut/unitary_res.csv", index=None)
