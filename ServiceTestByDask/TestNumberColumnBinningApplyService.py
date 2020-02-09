#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import dask.dataframe as dd

from ServiceImplByDask.CategoryColumnDataProfilingService import CategoryColumnDataProfilingService
from ServiceImplByDask.NumberColumnDataProfilingService import NumberColumnDataProfilingService
from ServiceImplByDask.NumberColumnBinningService import NumberColumnBinningService
from ServiceImplByDask.LowCardColumnFilteringService import LowCardColumnFilteringService
from ServiceImplByDask.RowPartitioningService import RowPartitioningService
from ServiceImplByDask.NumberColumnBinningApplyService import NumberColumnBinningApplyService


def main():
    data_dtypes = {'BILL_AMT1': np.float32,
                   'BILL_AMT2': np.float32, 'BILL_AMT3': np.float32, 'BILL_AMT4': np.float32, 'BILL_AMT5': np.float32,
                   'BILL_AMT6': np.float32, 'PAY_AMT1': np.float32, 'PAY_AMT2': np.float32, 'PAY_AMT3': np.float32,
                   'PAY_AMT4': np.float32, 'PAY_AMT5': np.float32, 'PAY_AMT6': np.float32, 'target': np.int32}
    data = dd.read_csv("E:/data/UCI_Credit_Card.csv", dtype=data_dtypes, usecols=data_dtypes.keys())

    # dic1 = dict()
    # dic1['data'] = data
    #
    # dic2 = dict()
    # dic2['nameofLabelCol'] = 'target'
    # dic2['seedOfRandom'] = 112
    # dic2['flagOfShuffle'] = True
    # dic2['ratioOfPartition'] = 0.2
    # dic2['flagOfStratify'] = True
    #
    # ps = RowPartitioningService()
    # dic3 = ps.process(dic1, dic2)
    #
    # p1 = dic3['partition1']
    # p2 = dic3['partition2']
    #
    # dic1 = dict()
    # dic1['dataOfNumberColumn'] = p1
    # dic2 = dict()
    # ncdp = NumberColumnDataProfilingService()
    # dic3 = ncdp.process(dic1, dic2)
    # dp = dic3['dataProfile']
    # print('p1 profile')
    # print(dp)
    #
    # dic1['data'] = dic1['dataOfNumberColumn']
    # dic1['dataProfile'] = dp
    # dic2 = dict()
    # dic2['thresholdOfCard'] = 20
    # lcnc = LowCardColumnFilteringService()
    # dic3 = lcnc.process(dic1, dic2)
    # print(dic3['dataOfLowCardColumn'].columns)
    # print(dic3['dataOfHighCardColumn'].columns)
    #
    # cols = dic3['dataOfHighCardColumn'].columns
    # data = dic3['dataOfHighCardColumn']
    # print("before bin")
    # print(data)
    dic1 = dict()
    dic1['dataOfNumberColumn'] = data
    dic2 = dict()
    # dic2['defaultBinningMethod'] = 'EqualWidth'
    # dic2['dictOfBinningMethod'] = {'PAY_AMT3': 'KMeans'}
    # dic2['dictOfBinningNumber'] = {'PAY_AMT4': 100}
    ncdp = NumberColumnBinningService()
    dic3 = ncdp.process(dic1, dic2)
    model = dic3['modelOfBinning']

    # dic1 = dict()
    # dic1['dataOfCategoryColumn'] = data
    # dic2 = dict()
    # ncdp = CategoryColumnDataProfilingService()
    # dic3 = ncdp.process(dic1, dic2)
    # dp = dic3['dataProfile']
    # print(dp)

    dic1 = dict()
    dic1['dataOfNumberColumn'] = data
    dic1['modelOfBinning'] = model
    dic2 = dict()
    ncdp = NumberColumnBinningApplyService()
    dic3 = ncdp.process(dic1, dic2)

    data = dic3['dataBinned']
    print(data)
    # dic1 = dict()
    # dic1['dataOfCategoryColumn'] = data
    # dic2 = dict()
    # ncdp = CategoryColumnDataProfilingService()
    # dic3 = ncdp.process(dic1, dic2)
    # dp = dic3['dataProfile']
    # print(dp)


if __name__ == '__main__':
    main()
