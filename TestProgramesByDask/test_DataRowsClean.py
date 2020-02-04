
import pandas as pd
import numpy as np
import dask.dataframe as dd
from unittest import TestCase

from IServiceImpByDask.DataRowsClean import DataRowsClean


class TestDataRowsClean(TestCase):
    def test_process(self):
        prepare_dtypes = {'CR_CRD_AC_AR_ID': np.str, 'CC_VLD_IND': np.uint16, 'CC_VLD_DT': np.str, 'CST_NM': np.str,
                          'FST_INTRO_EP_IP_ID': np.str, 'FST_INTRO_NM': np.str, 'FST_INTRO_LVL_CD': np.str,
                          'SCD_INTRO_EP_IP_ID': np.float32, 'SCD_INTRO_NM': np.str, 'SCD_INTRO_LVL_CD': np.str,
                          'FST_OD_AMT': np.float32, 'SCD_OD_AMT': np.float32}
        dataDf = dd.read_csv(r"E:\PythonProject\tgtks\TestByDaskResData\splittedTrainSet.csv", dtype=prepare_dtypes,
                                usecols=prepare_dtypes.keys())
        input = dict()
        input['inputDf'] = dataDf

        params = dict()
        params['mapping'] = {'MissRate': 0.126}

        bs = DataRowsClean()
        outputDict = bs.process(input, params)
        outDf = outputDict['outputDf']
        outDf.to_csv(r"E:\PythonProject\tgtks\TestByDaskResData\RowCleanData.csv", index=None)
