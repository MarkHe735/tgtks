#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import dask.dataframe as dd

from ServiceImplByDask.LowCardColumnFilteringService import LowCardColumnFilteringService
from ServiceImplByDask.CategoryColumnDataProfilingService import CategoryColumnDataProfilingService
from ServiceImplByDask.ColumnPartitioningByTypeService import ColumnPartitioningByTypeService


def main():
    data_dtypes = {'ID': np.int32, 'LIMIT_BAL': np.float32, 'SEX': np.str, 'EDUCATION': np.str,
                   'MARRIAGE': np.str, 'AGE': np.str, 'PAY_0': np.str, 'PAY_2': np.str, 'PAY_3': np.str,
                   'PAY_4': np.str, 'PAY_5': np.str, 'PAY_6': np.str, 'BILL_AMT1': np.float32,
                   'BILL_AMT2': np.float32, 'BILL_AMT3': np.float32, 'BILL_AMT4': np.float32, 'BILL_AMT5': np.float32,
                   'BILL_AMT6': np.float32, 'PAY_AMT1': np.float32, 'PAY_AMT2': np.float32, 'PAY_AMT3': np.float32,
                   'PAY_AMT4': np.float32, 'PAY_AMT5': np.float32, 'PAY_AMT6': np.float32, 'target': np.int32}
    data = dd.read_csv("E:/data/UCI_Credit_Card.csv", dtype=data_dtypes, usecols=data_dtypes.keys())
    print(data)
    dic1 = dict()
    dic1['data'] = data

    dic2 = dict()
    dic2['colsExcluded'] = ['sku']

    cps = ColumnPartitioningByTypeService()
    dic3 = cps.process(dic1, dic2)
    print(dic3['partitionOfNumberColumns'].columns)
    print(dic3['partitionOfCategoryColumns'].columns)
    print(dic3['partitionOfDatetimeColumns'].columns)
    print(dic3['partitionOfOtherColumns'].columns)

    dic1 = dict()
    dic1['dataOfCategoryColumn'] = dd.from_pandas(dic3['partitionOfCategoryColumns'], npartitions=2)

    dic2 = dict()
    ncdp = CategoryColumnDataProfilingService()
    dic3 = ncdp.process(dic1, dic2)
    print(dic3['dataProfile'])

    dic1['data'] = dd.from_pandas(dic1['dataOfCategoryColumn'], npartitions=2)
    dic1['dataProfile'] = dic3['dataProfile']
    dic2 = dict()
    dic2['thresholdOfCard'] = 4

    lcnc = LowCardColumnFilteringService()
    dic3 = lcnc.process(dic1, dic2)
    print(dic3['dataOfLowCardColumn'].columns)
    print(dic3['dataOfHighCardColumn'].columns)


if __name__ == '__main__':
    main()
