#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from ServiceImplByPandas.ColumnPartitioningByTypeService import ColumnPartitioningByTypeService
from ServiceImplByPandas.CategoryColumnDataProfilingService import CategoryColumnDataProfilingService


def main():
    data = pd.read_csv("/home/user/german_credit.csv")
    dic1 = dict()
    dic1['data'] = data
    dic2 = dict()
    cps = ColumnPartitioningByTypeService()
    dic3 = cps.process(dic1, dic2)
    print(dic3)
    data1 = dic3['partitionOfCategoryColumns']

    dic1 = dict()
    dic1['dataOfCategoryColumn'] = data1
    dic2 = dict()
    qps = CategoryColumnDataProfilingService()
    dic3 = qps.process(dic1, dic2)
    print(dic3['dataProfile'])


if __name__ == '__main__':
    main()
