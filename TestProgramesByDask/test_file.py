#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:mark
# datetime:2019/12/19 14:00
# software: PyCharm
import json

import dask
import pandas as pd
import dask.dataframe as dd
import dask.array as da
from dask.diagnostics import ProgressBar
import numpy as np
from scipy import stats
from scipy.stats import anderson

prepare_dtypes = {'CR_CRD_AC_AR_ID': np.str, 'CC_VLD_IND': np.uint16, 'CC_VLD_DT': np.str, 'CST_NM': np.str,
                  'FST_INTRO_EP_IP_ID': np.str, 'FST_INTRO_NM': np.str, 'FST_INTRO_LVL_CD': np.str,
                  'SCD_INTRO_EP_IP_ID': np.float32, 'SCD_INTRO_NM': np.str, 'SCD_INTRO_LVL_CD': np.str,
                  'FST_OD_AMT': np.float32, 'SCD_OD_AMT': np.float32}
dataframe = dd.read_csv(r"E:\PythonProject\tgtks\TestByDaskResData\splittedTrainSet.csv", dtype=prepare_dtypes, usecols=prepare_dtypes.keys())


# useless parameter, to unify the method invoking.
params = dict()
params['mapping'] = {'SCD_OD_AMT': 'mean', 'FST_OD_AMT': 'median', 'SCD_INTRO_EP_IP_ID': 'median',
                     'FST_INTRO_NM': 'mode', 'SCD_INTRO_NM': 'mode', 'FST_INTRO_LVL_CD': 'mode',
                     'SCD_INTRO_LVL_CD': 'mode', 'FST_INTRO_EP_IP_ID': 'mode', 'CST_NM': 'mode'}

dataType = dataframe.dtypes
dataTypeDf = pd.DataFrame({'Columns': dataType.index, 'Dtypes': dataType.values})

# 将列分为数值型与字符串型（未来需考虑时间格式）
col_types = dataTypeDf.loc[:, 'Dtypes']
colNum = dataTypeDf.loc[(col_types != 'object')
                        & (col_types != 'category')
                        & (col_types != 'datetime'), 'Columns'].to_list()
colStr = dataTypeDf.loc[col_types == 'object', 'Columns'].tolist()
# print(colNum)
# print(colStr)
# print(dataTypeDf.loc[:, 'Dtypes'])
empyt_value = None
value_dict = {'MissRate': [], 'UniqueRate': [], 'Max': [], 'Min': [], 'Mean': [], 'Median': [], 'Mode': [], 'ZeroNum': []}
for col in dataframe.columns:
    # value_dict['Columns'].append(col)
    # value_dict['MissRate'].append(dataframe[col].isnull().mean().compute())
    # value_dict['UniqueRate'].append((dataframe[col].value_counts().size / dataframe.index.size).compute())
    if col in colNum:
        value_dict['Max'].append(np.max(dataframe[col]))
        # value_dict['Min'].append(da.min(dataframe[col]).compute())
        # value_dict['Mean'].append(da.mean(dataframe[col]).compute())
        # value_dict['Median'].append(da.median(dataframe[col]).compute())
        # value_dict['ZeroNum'].append(len(dataframe[col][dataframe[col] == 0]))
    else:
        value_dict['Max'].append(empyt_value)
        # value_dict['Min'].append(empyt_value)
        # value_dict['Mean'].append(empyt_value)
        # value_dict['Median'].append(empyt_value)
        # value_dict['ZeroNum'].append(empyt_value)

    # if col in colStr:
    #     value_dict['Mode'].append(stats.mode(dataframe[col])[0][0])
    # else:
    #     value_dict['Mode'].append(empyt_value)


print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&",value_dict)
