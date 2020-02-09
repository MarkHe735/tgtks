#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import dask.dataframe as dd
from ServiceImplByDask.LowCardColumnFilteringService import LowCardColumnFilteringService
from ServiceImplByDask.NumberColumnDataProfilingService import NumberColumnDataProfilingService
from ServiceImplByDask.ColumnPartitioningByTypeService import ColumnPartitioningByTypeService
from ServiceImplByDask.ToCategoryConvertingService import ToCategoryConvertingService
from ServiceImplByDask.CategoryColumnMissingValueImputingService import CategoryColumnMissingValueImputingService


def main():
    data_dtypes = {'BILL_AMT1': np.float32,
                   'BILL_AMT2': np.float32, 'BILL_AMT3': np.float32, 'BILL_AMT4': np.float32, 'BILL_AMT5': np.float32,
                   'BILL_AMT6': np.float32, 'PAY_AMT1': np.float32, 'PAY_AMT2': np.float32, 'PAY_AMT3': np.float32,
                   'PAY_AMT4': np.float32, 'PAY_AMT5': np.float32, 'PAY_AMT6': np.float32, 'target': np.int32}
    data = dd.read_csv("E:/data/UCI_Credit_Card.csv", dtype=data_dtypes, usecols=data_dtypes.keys())

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

    # dic1['data'] = dic1['dataOfNumberColumn']
    # dic1['dataProfile'] = dic3['dataProfile']
    # dic2 = dict()
    # dic2['thresholdOfCard'] = 4
    # lcnc = LowCardColumnFilteringService()
    # dic3 = lcnc.process(dic1, dic2)
    #
    # dic1 = dict()
    # dic1['data'] = dic3['dataOfLowCardColumn']
    # dic2 = dict()
    # ncdp = ToCategoryConvertingService()
    # dic3 = ncdp.process(dic1, dic2)
    # print(dic3['dataOfCateorgyColumn'])

    print("-------------------")

    dic1 = dict()
    dic1['dataOfCategoryColumn'] = data
    dic1['dataProfile'] = dataProfile
    dic2 = dict()
    md = dict()
    md['EDUCATION'] = 'const_LOW'
    dic2['dictOfMethod'] = md
    ncdp = CategoryColumnMissingValueImputingService()
    dic3 = ncdp.process(dic1, dic2)
    print(dic3['dataWithoutMissingValue'])


if __name__ == '__main__':
    main()
