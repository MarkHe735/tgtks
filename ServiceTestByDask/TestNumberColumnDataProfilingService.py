#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import dask.dataframe as dd

from ServiceImplByDask.ColumnPartitioningByTypeService import ColumnPartitioningByTypeService
from ServiceImplByDask.NumberColumnDataProfilingService import NumberColumnDataProfilingService


def main():
    data_dtypes = {'ID': np.int32, 'LIMIT_BAL': np.float32, 'SEX': np.int32, 'EDUCATION': np.int32,
                   'MARRIAGE': np.int32, 'AGE': np.int32, 'PAY_0': np.int32, 'PAY_2': np.int32, 'PAY_3': np.int32,
                   'PAY_4': np.int32, 'PAY_5': np.int32, 'PAY_6': np.int32, 'BILL_AMT1': np.float32,
                   'BILL_AMT2': np.float32, 'BILL_AMT3': np.float32, 'BILL_AMT4': np.float32, 'BILL_AMT5': np.float32,
                   'BILL_AMT6': np.float32, 'PAY_AMT1': np.float32, 'PAY_AMT2': np.float32, 'PAY_AMT3': np.float32,
                   'PAY_AMT4': np.float32, 'PAY_AMT5': np.float32, 'PAY_AMT6': np.float32, 'target': np.int32}
    data = dd.read_csv("E:/data/UCI_Credit_Card.csv", dtype=data_dtypes, usecols=data_dtypes.keys())
    dic1 = dict()
    dic1['data'] = data
    dic2 = dict()
    cps = ColumnPartitioningByTypeService()
    dic3 = cps.process(dic1, dic2)
    data1 = dic3['partitionOfNumberColumns']

    dic1 = dict()
    dic1['dataOfNumberColumn'] = dd.from_pandas(data1, npartitions=2)
    dic2 = dict()
    qps = NumberColumnDataProfilingService()
    dic3 = qps.process(dic1, dic2)
    print(dic3['dataProfile'][['normal', 'lognormal', 'card']])


if __name__ == '__main__':
    main()
