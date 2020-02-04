from unittest import TestCase
import numpy as np
import dask.dataframe as dd

from IServiceImpByDask.TrainTestSplitter import TrainTestSplitter


class TestTrainTestSplitter(TestCase):
    def test_process(self):
        data_dtypes = {'ID': np.int32, 'LIMIT_BAL': np.float32, 'SEX': np.int32, 'EDUCATION': np.int32,
                       'MARRIAGE': np.int32, 'AGE': np.int32, 'PAY_0': np.int32, 'PAY_2': np.int32, 'PAY_3': np.int32,
                       'PAY_4': np.int32, 'PAY_5': np.int32, 'PAY_6': np.int32, 'BILL_AMT1': np.float32,
                       'BILL_AMT2': np.float32, 'BILL_AMT3': np.float32, 'BILL_AMT4': np.float32,
                       'BILL_AMT5': np.float32,
                       'BILL_AMT6': np.float32, 'PAY_AMT1': np.float32, 'PAY_AMT2': np.float32, 'PAY_AMT3': np.float32,
                       'PAY_AMT4': np.float32, 'PAY_AMT5': np.float32, 'PAY_AMT6': np.float32, 'target': np.int32}
        dataDf = dd.read_csv("E:/data/UCI_Credit_Card.csv", dtype=data_dtypes, usecols=data_dtypes.keys())
        # prepare_dtypes = {'CR_CRD_AC_AR_ID': np.str, 'CC_VLD_IND': np.uint16, 'CC_VLD_DT': np.str, 'CST_NM': np.str,
        #                   'FST_INTRO_EP_IP_ID': np.str, 'FST_INTRO_NM': np.str, 'FST_INTRO_LVL_CD': np.str,
        #                   'SCD_INTRO_EP_IP_ID': np.float32, 'SCD_INTRO_NM': np.str, 'SCD_INTRO_LVL_CD': np.str,
        #                   'FST_OD_AMT': np.float32, 'SCD_OD_AMT': np.float32}
        # dataDf = dd.read_csv(r'E:\PythonProject\tgtks\data\dask_test_data.csv', dtype=prepare_dtypes, usecols=prepare_dtypes.keys())
        # prepare_dtypes = {'ACG_DT': np.str, 'CR_CRD_AC_AR_ID': np.str, 'MAIN_CRD_NO': np.str, 'CCY_CD': np.str,
        #                   'PD_TP_CD': np.str, 'CR_CRD_STS_CD': np.str, 'AC_STS_CD': np.str, 'AC_STS_DT': np.str,
        #                   'WRT_OFF_IND': np.str, 'CC_VLD_IND': np.uint16, 'CC_VLD_DT': np.str, 'CR_LMT_AMT': np.str,
        #                   'CC_FIVE_CTG_CD': np.str, 'ACS_ORG_ID': np.str, 'ACG_ORG_ID': np.str, 'BIL_DAY': np.str,
        #                   'AC_OPN_DT': np.str, 'RLT_RPAY_AC': np.str, 'BAL': np.str, 'OVFL_PYMT_AMT': np.str,
        #                   'CST_BAL': np.str, 'CSH_ADV_BAL': np.str, 'LST_TM_AC_STM_AMT': np.str, 'OVDR_BAL': np.str,
        #                   'OD_TM': np.str, 'OD_AMT': np.str, 'ON_SHT_OVDR_BAL': np.str, 'OFF_SHT_OVDR_BAL': np.str,
        #                   'DLQ_PNP_BAL': np.str, 'DLQ_INT_BAL': np.str, 'DLQ_FEE_BAL': np.str,
        #                   'LTST_TM_LWST_SHD_RPAY_AMT': np.str, 'RCVB_INT_AMT': np.str, 'CMPD_BAL': np.str,
        #                   'INT_PNT_BAL': np.str, 'CSH_CMSN_FEE_BAL': np.str, 'NONE_FLL_AMT_RPAY_CMSN_FEE_BAL': np.str,
        #                   'YR_FEE_BAL': np.str, 'INSTL_PYMT_CMSN_FEE_BAL': np.str, 'PRDTN_FEE_BAL': np.str,
        #                   'OVER_LMT_FEE_BAL': np.str, 'OTR_FEE_BAL': np.str, 'ON_SHT_RCVB_CST_AMT': np.str,
        #                   'OFF_SHT_RCVB_CST_AMT': np.str, 'CST_ID': np.str, 'CST_NM': np.str,
        #                   'FST_INTRO_EP_IP_ID': np.str, 'FST_INTRO_NM': np.str, 'FST_INTRO_LVL_CD': np.str,
        #                   'SCD_INTRO_EP_IP_ID': np.str, 'SCD_INTRO_NM': np.str, 'SCD_INTRO_LVL_CD': np.str,
        #                   'FST_OD_AMT': np.str, 'SCD_OD_AMT': np.str, 'THD_OD_AMT': np.str, 'FOTH_OD_AMT': np.str,
        #                   'FTH_OD_AMT': np.str, 'STH_OD_AMT': np.str, 'CR_LMT_AMT_SH': np.str, 'TMP_LMT_AMT': np.str,
        #                   'TMP_LMT_VLD_DT': np.str, 'TMP_LMT_IVLD_DT': np.str, 'LST_BIL_MIN_DUE_IND': np.str}
        # dataDf = dd.read_csv(r'F:\Data_user\泰隆客户流失\XinYongKa.csv', dtype=prepare_dtypes,
        #                      usecols=prepare_dtypes.keys())

        input = dict()
        input['inputDf'] = dataDf

        params = dict()
        params['mapping'] = {'labelCol': 'target', 'testRatio': 0.3}

        bs = TrainTestSplitter()
        outputDict = bs.process(input, params)
        train_df = outputDict['outputDf1']
        test_df = outputDict['outputDf2']
        train_df.to_csv(r"E:\PythonProject\tgtks\TestByDaskResData\splittedTrainSet.csv", index=None)
        test_df.to_csv(r"E:\PythonProject\tgtks\TestByDaskResData\splittedTestSet.csv", index=None)
