#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import dask.dataframe as dd

from ServiceImplByDask.NumberColumnBinningService import NumberColumnBinningService


def main():
    data_dtypes = {'BILL_AMT1': np.float32,
                   'BILL_AMT2': np.float32, 'BILL_AMT3': np.float32, 'BILL_AMT4': np.float32, 'BILL_AMT5': np.float32,
                   'BILL_AMT6': np.float32, 'PAY_AMT1': np.float32, 'PAY_AMT2': np.float32, 'PAY_AMT3': np.float32,
                   'PAY_AMT4': np.float32, 'PAY_AMT5': np.float32, 'PAY_AMT6': np.float32, 'target': np.int32}
    data = dd.read_csv("E:/data/UCI_Credit_Card.csv", dtype=data_dtypes, usecols=data_dtypes.keys())

    # dic1 = dict()
    # dic1['dataOfNumberColumn'] = data
    #
    # dic2 = dict()
    # ncdp = NumberColumnDataProfilingService()
    # dic3 = ncdp.process(dic1, dic2)
    # dp = dic3['dataProfile']
    # print(dp)

    # dic1['data'] = dic1['dataOfNumberColumn']
    # dic1['dataProfile'] = dp
    # dic2 = dict()
    # dic2['thresholdOfCard'] = 20
    # lcnc = LowCardColumnFilteringService()
    # dic3 = lcnc.process(dic1, dic2)
    # print(dic3['dataOfHighCardColumn'].columns)
    # print(dic3['dataOfHighCardColumn'])

    dic1 = dict()
    dic1['dataOfNumberColumn'] = data
    # TODO: 控制参数与组件接口定义不匹配
    dic2 = dict()
    # dic2['defaultBinningMethod'] = 'EqualFreq'
    # dic2['dictOfBinningMethod'] = {'PAY_AMT3': 'KMeans'}
    # dic2['dictOfBinningNumber'] = {'PAY_AMT4': 100}
    ncdp = NumberColumnBinningService()
    dic3 = ncdp.process(dic1, dic2)
    print(dic3['modelOfBinning'])


if __name__ == '__main__':
    main()
