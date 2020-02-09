#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import dask.dataframe as dd

from ServiceImplByDask.NumberColumnDataProfilingService import NumberColumnDataProfilingService
from ServiceImplByDask.ColumnPartitioningByTypeService import ColumnPartitioningByTypeService
from ServiceImplByDask.NumberColumnMissingValueImputingService import NumberColumnMissingValueImputingService


def main():
    data_dtypes = {'BILL_AMT1': np.float32,
                   'BILL_AMT2': np.float32, 'BILL_AMT3': np.float32, 'BILL_AMT4': np.float32, 'BILL_AMT5': np.float32,
                   'BILL_AMT6': np.float32, 'PAY_AMT1': np.float32, 'PAY_AMT2': np.float32, 'PAY_AMT3': np.float32,
                   'PAY_AMT4': np.float32, 'PAY_AMT5': np.float32, 'PAY_AMT6': np.float32, 'target': np.int32}
    data = dd.read_csv("E:/data/UCI_Credit_Card.csv", dtype=data_dtypes, usecols=data_dtypes.keys())

    print(data.describe())
    print(data)

    # dic1 = dict()
    # dic1['data'] = data
    # dic2 = dict()
    # cps = ColumnPartitioningByTypeService()
    # dic3 = cps.process(dic1, dic2)

    dic1 = dict()
    dic1['dataOfNumberColumn'] = data
    dic2 = dict()
    ncdp = NumberColumnDataProfilingService()
    dic3 = ncdp.process(dic1, dic2)
    print(dic3['dataProfile'])
    dataProfile = dic3['dataProfile']

    dic1 = dict()
    dic1['dataOfNumberColumn'] = data
    dic1['dataProfile'] = dataProfile
    dic2 = dict()
    md = dict()
    md['LIMIT_BAL'] = 'const_112.3333'
    dic2['dictOfMethod'] = md
    ncdp = NumberColumnMissingValueImputingService()
    dic3 = ncdp.process(dic1, dic2)
    print('=== Result: ===')
    print(dic3['dataWithoutMissingValue'])


if __name__ == '__main__':
    main()
