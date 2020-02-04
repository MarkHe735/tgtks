#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import dask.dataframe as dd

from ServiceImplByDask.QualityProfilingService import QualityProfilingService
from ServiceImplByDask.ColumnCleaningService import ColumnCleaningService


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
    print(data.shape)

    dic2 = dict()

    qps = QualityProfilingService()
    dic3 = qps.process(dic1, dic2)
    dqp = dic3['qualityProfile']
    print(dqp)

    dic1 = dict()
    dic1['data'] = data
    dic1['qualityProfile'] = dqp
    dic2 = dict()

    dic2['thresholdOfMissing'] = 0.7
    dic2['thresholdOfIdLikeness'] = 0.5
    dic2['thresholdOfConstLikeness'] = 0.7

    dccs = ColumnCleaningService()
    dic3 = dccs.process(dic1, dic2)
    print(dic3['dataCleaned'])


if __name__ == '__main__':
    main()
