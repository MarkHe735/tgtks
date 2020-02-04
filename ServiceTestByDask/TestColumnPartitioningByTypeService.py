#!/usr/bin/env python
# -*- coding: utf-8 -*-

import dask.dataframe as dd
import numpy as np

from ServiceImplByDask.ColumnPartitioningByTypeService import ColumnPartitioningByTypeService


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
    print(data.dtypes)

    dic2 = dict()
    dic2['colsIncluded'] = ['ID', 'target']

    cps = ColumnPartitioningByTypeService()
    dic3 = cps.process(dic1, dic2)
    print(dic3['partitionOfNumberColumns'].columns)
    print(dic3['partitionOfCategoryColumns'].columns)
    print(dic3['partitionOfDatetimeColumns'].columns)
    print(dic3['partitionOfOtherColumns'].columns)


if __name__ == '__main__':
    main()
