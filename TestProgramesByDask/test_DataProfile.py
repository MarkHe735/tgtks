from unittest import TestCase
import numpy as np
import dask.dataframe as dd

from IServiceImpByDask.DataProfile import DataProfile


class TestDataProfile(TestCase):
    def test_process(self):
        prepare_dtypes = {'CR_CRD_AC_AR_ID': np.str, 'CC_VLD_IND': np.uint16, 'CC_VLD_DT': np.str, 'CST_NM': np.str,
                          'FST_INTRO_EP_IP_ID': np.str, 'FST_INTRO_NM': np.str, 'FST_INTRO_LVL_CD': np.str,
                          'SCD_INTRO_EP_IP_ID': np.float32, 'SCD_INTRO_NM': np.str, 'SCD_INTRO_LVL_CD': np.str,
                          'FST_OD_AMT': np.float32, 'SCD_OD_AMT': np.float32}
        dataDf = dd.read_csv(r"E:\PythonProject\tgtks\TestByDaskResData\splittedTrainSet.csv", dtype=prepare_dtypes,
                                usecols=prepare_dtypes.keys())

        input = dict()
        input['inputDf'] = dataDf

        # useless parameter, to unify the method invoking.
        params = dict()
        params['mapping'] = {'SCD_OD_AMT': 'mean', 'FST_OD_AMT': 'median', 'SCD_INTRO_EP_IP_ID': 'median',
                             'FST_INTRO_NM': 'mode', 'SCD_INTRO_NM': 'mode', 'FST_INTRO_LVL_CD': 'mode',
                             'SCD_INTRO_LVL_CD': 'mode', 'FST_INTRO_EP_IP_ID': 'mode', 'CST_NM': 'mode'}

        bs = DataProfile()
        outputDict = bs.process(input, params)
        outDf1 = outputDict['outputDf']
        outDf1.to_csv(r"E:\PythonProject\tgtks\TestByDaskResData\ProfileResData.csv", index=None)
        # running 55 minutes for current test data set
