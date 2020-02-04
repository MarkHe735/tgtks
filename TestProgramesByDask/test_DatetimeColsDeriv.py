from unittest import TestCase
import numpy as np
import pandas as pd
import dask.dataframe as dd
from datetime import datetime

from IServiceImpByDask.DatetimeColsDeriv import DatetimeColsDeriv


class TestDatetimeColsDeriv(TestCase):
    def test_process(self):
        dateparse = lambda dates: datetime.strptime(dates, '%Y-%m-%d')
        prepare_dtypes = {'CR_CRD_AC_AR_ID': np.str, 'CC_VLD_IND': np.uint16, 'CC_VLD_DT': np.str, 'CST_NM': np.str,
                          'FST_INTRO_EP_IP_ID': np.str, 'FST_INTRO_NM': np.str, 'FST_INTRO_LVL_CD': np.str,
                          'SCD_INTRO_EP_IP_ID': np.float32, 'SCD_INTRO_NM': np.str, 'SCD_INTRO_LVL_CD': np.str,
                          'FST_OD_AMT': np.float32, 'SCD_OD_AMT': np.float32}
        dataDf = dd.read_csv(r"E:\PythonProject\tgtks\TestByDaskResData\DateDf.csv",
                             parse_dates=['CC_VLD_DT'], date_parser=dateparse, dtype=prepare_dtypes)
        input = dict()
        input['inputDf'] = dataDf

        params = dict()
        params['mapping'] = {'idCol': 'CR_CRD_AC_AR_ID'}

        bs = DatetimeColsDeriv()
        outputDict = bs.process(input, params)
        outDf1 = outputDict['outputDf1']
        outDf1.to_csv(r"E:\PythonProject\tgtks\TestByDaskResData\date_df.csv", index=None)
