#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import dask.dataframe as dd

from ServiceImplByDask.ToCategoryConvertingService import ToCategoryConvertingService


def main():
    data_dtypes = {'PAY_AMT1': np.float32, 'PAY_AMT2': np.float32, 'PAY_AMT3': np.float32,
                   'PAY_AMT4': np.float32, 'PAY_AMT5': np.float32, 'PAY_AMT6': np.float32, 'target': np.int32}
    data = dd.read_csv("E:/data/UCI_Credit_Card.csv", dtype=data_dtypes, usecols=data_dtypes.keys())

    dic1 = dict()
    dic1['data'] = data

    dic2 = dict()
    ncdp = ToCategoryConvertingService()
    dic3 = ncdp.process(dic1, dic2)
    print(dic3['dataOfCateorgyColumn'].head(10))
    print(dic3['dataOfCateorgyColumn'].dtypes)


if __name__ == '__main__':
    main()
