#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ServiceImplByPandas.ColumnPartitioningByTypeService import ColumnPartitioningByTypeService
from ServiceImplByPandas.NumberColumnDataProfilingService import NumberColumnDataProfilingService

import pandas as pd


def main():

    data = pd.read_csv("/home/user/test.csv")
    dic1 = dict()
    dic1['data'] = data
    dic2 = dict()
    cps = ColumnPartitioningByTypeService()
    dic3 = cps.process(dic1, dic2)
    data1 = dic3['partitionOfNumberColumns']

    dic1 = dict()
    dic1['dataOfNumberColumn'] = data1
    dic2 = dict()
    qps = NumberColumnDataProfilingService()
    dic3 = qps.process(dic1, dic2)
    print(dic3['dataProfile'][['normal','lognormal','card']])

if __name__ == '__main__':
    main()