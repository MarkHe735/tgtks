#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import dask.dataframe as dd

from ServiceImplByDask.RowPartitioningService import RowPartitioningService


def main():
    data_dtypes = {'ID': np.int32, 'LIMIT_BAL': np.float32, 'SEX': np.int32, 'EDUCATION': np.int32,
                   'MARRIAGE': np.int32, 'AGE': np.int32, 'PAY_0': np.int32, 'PAY_2': np.int32, 'PAY_3': np.int32,
                   'PAY_4': np.int32, 'PAY_5': np.int32, 'PAY_6': np.int32, 'BILL_AMT1': np.float32,
                   'BILL_AMT2': np.float32, 'BILL_AMT3': np.float32, 'BILL_AMT4': np.float32, 'BILL_AMT5': np.float32,
                   'BILL_AMT6': np.float32, 'PAY_AMT1': np.float32, 'PAY_AMT2': np.float32, 'PAY_AMT3': np.float32,
                   'PAY_AMT4': np.float32, 'PAY_AMT5': np.float32, 'PAY_AMT6': np.float32, 'target': np.int32}
    data = dd.read_csv("E:/data/UCI_Credit_Card.csv", dtype=data_dtypes, usecols=data_dtypes.keys())
    # 大数据集测试通过
    data_dtypes = {'ZHANGHAO': np.str, 'JIEDAIBZ': np.str, 'JIAOYBIZ': np.str, 'JIAOYIJE': np.str, 'ZHAIYODM': np.str,
                   'ZHAIYOMS': np.str, 'QDAOLEIX': np.str, 'JIAOYIRQ': np.float32}
    data = dd.read_csv(r'E:\data\CunKuanZhangYe.csv', dtype=data_dtypes,
                       usecols=data_dtypes.keys())
    # print(data.dtypes)
    # print(data.shape)

    data['JIEDAIBZ'] = data['JIEDAIBZ'].apply(lambda x: 1 if x == 'D' else 0, meta=('JIEDAIBZ', 'int32'))

    dic1 = dict()
    dic1['data'] = data

    dic2 = dict()
    dic2['nameofLabelCol'] = 'JIEDAIBZ'

    dic2['seedOfRandom'] = 112
    dic2['flagOfShuffle'] = True
    dic2['ratioOfPartition'] = 0.5

    ps = RowPartitioningService()
    dic3 = ps.process(dic1, dic2)

    p1 = dic3['partition1']
    p2 = dic3['partition2']
    print(p1.dtypes)
    print(p1.shape)
    print(p2.shape)
    print(p2)


if __name__ == '__main__':
    main()
