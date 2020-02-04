from unittest import TestCase
import numpy as np
import pandas as pd
import dask.dataframe as dd

from IServiceImpByDask.OutlierProcess import OutlierProcess


class TestOutlierProcess(TestCase):
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
        params['mapping'] = {'colsMap': {'FST_OD_AMT': 'IQR', 'SCD_OD_AMT': 'sigma'},
                             # 用于判断正态分布相似度:
                             'point': 15.0}

        bs = OutlierProcess()
        outputDict = bs.process(input, params)
        outDf = outputDict['outputDf']
        outDf.to_csv(r"E:\PythonProject\tgtks\TestByDaskResData\OutlierData.csv", index=None)
        pass
