#!/usr/bin/env python
# -*- coding: utf-8 -*-

import dask.dataframe as dd
import numpy as np

from ServiceImplByDask.ColumnPartitioningByTypeService import ColumnPartitioningByTypeService
from ServiceImplByDask.CategoryColumnDataProfilingService import CategoryColumnDataProfilingService


def main():
    data_dtypes = {'ID': np.int32, 'LIMIT_BAL': np.str, 'SEX': np.str, 'EDUCATION': np.str,
                   'MARRIAGE': np.str, 'AGE': np.str, 'PAY_0': np.str, 'PAY_2': np.str, 'PAY_3': np.str,
                   'PAY_4': np.str, 'PAY_5': np.str, 'PAY_6': np.str, 'BILL_AMT1': np.str,
                   'BILL_AMT2': np.float32, 'BILL_AMT3': np.float32, 'BILL_AMT4': np.float32, 'BILL_AMT5': np.float32,
                   'BILL_AMT6': np.float32, 'PAY_AMT1': np.float32, 'PAY_AMT2': np.float32, 'PAY_AMT3': np.float32,
                   'PAY_AMT4': np.float32, 'PAY_AMT5': np.float32, 'PAY_AMT6': np.float32, 'target': np.int32}
    data = dd.read_csv("E:/data/UCI_Credit_Card.csv", dtype=data_dtypes, usecols=data_dtypes.keys())
    # dic1 = dict()
    # dic1['data'] = data
    # dic2 = dict()
    # cps = ColumnPartitioningByTypeService()
    # dic3 = cps.process(dic1, dic2)
    # print(dic3)
    # data1 = dic3['partitionOfCategoryColumns']

    data = data[['LIMIT_BAL', 'SEX', 'EDUCATION', 'MARRIAGE']]

    dic1 = dict()
    dic1['dataOfCategoryColumn'] = data
    dic2 = dict()
    qps = CategoryColumnDataProfilingService()
    dic3 = qps.process(dic1, dic2)
    print(dic3['dataProfile'])


if __name__ == '__main__':
    main()
