from unittest import TestCase
import pandas as pd
import dask.dataframe as dd
import numpy as np

from IServiceImpByDask.DataFillNA import DataFillNA


class TestDataFileNA(TestCase):
    def test_process(self):
        prepare_dtypes = {'CR_CRD_AC_AR_ID': np.str, 'CC_VLD_IND': np.uint16, 'CC_VLD_DT': np.str, 'CST_NM': np.str,
                          'FST_INTRO_EP_IP_ID': np.str, 'FST_INTRO_NM': np.str, 'FST_INTRO_LVL_CD': np.str,
                          'SCD_INTRO_EP_IP_ID': np.float32, 'SCD_INTRO_NM': np.str, 'SCD_INTRO_LVL_CD': np.str,
                          'FST_OD_AMT': np.float32, 'SCD_OD_AMT': np.float32}
        dataDf = dd.read_csv(r"E:\PythonProject\tgtks\TestByDaskResData\splittedTrainSet.csv", dtype=prepare_dtypes,
                                usecols=prepare_dtypes.keys())
        profileDf = pd.read_csv(r"E:\PythonProject\tgtks\TestByDaskResData\ProfileResData.csv", encoding='utf-8')
        input = dict()
        input['inputDf1'] = dataDf
        input['inputDf2'] = profileDf

        params = dict()
        params['mapping'] = {'defaultFill': {'num': 'Mean', 'enum': 'Mode', 'datetime': 'bfill'},
                             'colsMap': {'SCD_OD_AMT': 'Max', 'FST_OD_AMT': 'CONST_0', 'SCD_INTRO_EP_IP_ID': 'CONST_1'}}

        bs = DataFillNA()
        outputDict = bs.process(input, params)
        outDf = outputDict['outputDf']
        outDf.to_csv(r"E:\PythonProject\tgtks\TestByDaskResData\FillNaData.csv", index=None)
