from unittest import TestCase
import dask.dataframe as dd
import pandas as pd
import numpy as np

from IServiceImpByDask.DataColsClean import DataColsClean


class TestDataColsClean(TestCase):
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
        control_paras = {'keepCols': ['CR_CRD_AC_AR_ID', 'CC_VLD_DT', 'CC_VLD_IND'],
                         'missThreshold': 0.4, 'uniqueThreshold': 0.9, 'constantThreshold': 0.8,
                         'missThresholdDict': {'SCD_INTRO_NM': 0.7, 'SCD_INTRO_LVL_CD': 0.6}}
        params['mapping'] = control_paras

        bs = DataColsClean()
        outputDict = bs.process(input, params)
        outDf = outputDict['outputDf']
        outDf.to_csv(r"E:\PythonProject\tgtks\TestByDaskResData\DelColumnsData.csv", index=None)
