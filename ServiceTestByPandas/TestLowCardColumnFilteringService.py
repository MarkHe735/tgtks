#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

from ServiceImplByPandas.LowCardColumnFilteringService import LowCardColumnFilteringService
from ServiceImplByPandas.CategoryColumnDataProfilingService import CategoryColumnDataProfilingService
from ServiceImplByPandas.ColumnPartitioningByTypeService import ColumnPartitioningByTypeService


def main():
    data = pd.read_csv("/home/user/german_credit.csv")
    print(data)
    dic1 = dict()
    dic1['data'] = data

    dic2 = dict()
    dic2['colsExcluded'] = ['sku']

    cps = ColumnPartitioningByTypeService()
    dic3 = cps.process(dic1, dic2)
    print(dic3['partitionOfNumberColumns'].columns)
    print(dic3['partitionOfCategoryColumns'].columns)
    print(dic3['partitionOfDatetimeColumns'].columns)
    print(dic3['partitionOfOtherColumns'].columns)

    dic1 = dict()
    dic1['dataOfCategoryColumn'] = dic3['partitionOfCategoryColumns']

    dic2 = dict()
    ncdp = CategoryColumnDataProfilingService()
    dic3 = ncdp.process(dic1, dic2)
    print(dic3['dataProfile'])

    dic1['data'] = dic1['dataOfCategoryColumn']
    dic1['dataProfile'] = dic3['dataProfile']
    dic2 = dict()
    dic2['thresholdOfCard'] = 4

    lcnc = LowCardColumnFilteringService()
    dic3 = lcnc.process(dic1, dic2)
    print(dic3['dataOfLowCardColumn'].columns)
    print(dic3['dataOfHighCardColumn'].columns)


if __name__ == '__main__':
    main()
