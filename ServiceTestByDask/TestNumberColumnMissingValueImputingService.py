#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd
from ServiceImplByPandas.NumberColumnDataProfilingService import NumberColumnDataProfilingService
from ServiceImplByPandas.ColumnPartitioningByTypeService import ColumnPartitioningByTypeService
from ServiceImplByPandas.NumberColumnMissingValueImputingService import NumberColumnMissingValueImputingService

def main():
    data = pd.read_csv("/home/user/test.csv")
    print(data.describe())
    print(data)
    dic1 = dict()
    dic1['data'] = data
    dic2 = dict()
    cps = ColumnPartitioningByTypeService()
    dic3 = cps.process(dic1, dic2)

    dic1 = dict()
    dic1['dataOfNumberColumn'] = dic3['partitionOfNumberColumns']
    dic2 = dict()
    ncdp = NumberColumnDataProfilingService()
    dic3 = ncdp.process(dic1, dic2)
    print(dic3['dataProfile'])
    dataProfile = dic3['dataProfile']

    dic1['dataProfile'] = dataProfile
    dic2 = dict()
    md = dict()
    md['LIMIT_BAL'] = 'const_112.3333'
    dic2['dictOfMethod'] = md
    ncdp = NumberColumnMissingValueImputingService()
    dic3 = ncdp.process(dic1, dic2)
    print(dic3['dataWithoutMissingValue'])


if __name__ == '__main__':
    main()
