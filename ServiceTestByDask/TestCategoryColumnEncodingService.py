#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import category_encoders as ce
import pandas as pd

from ServiceImplByPandas.CategoryColumnDataProfilingService import CategoryColumnDataProfilingService
from ServiceImplByPandas.ColumnPartitioningByTypeService import ColumnPartitioningByTypeService
from ServiceImplByPandas.CategoryColumnEncodingService import CategoryColumnEncodingService


def main():
    data = pd.read_csv("/home/user/UCI_Credit_Card.csv")
    dic1 = dict()
    data = data[['ID', 'EDUCATION']]
    data = data.astype('str')
    dic1['data'] = data
    print(data.dtypes)

    dic2 = dict()
    dic2['colsExcluded'] = ['ID']

    cps = ColumnPartitioningByTypeService()
    dic3 = cps.process(dic1, dic2)
    data1 = dic3['partitionOfCategoryColumns']
    print(data1)

    dic1 = dict()
    dic1['dataOfCategoryColumn'] = data1
    dic2 = dict()
    qps = CategoryColumnDataProfilingService()
    dic3 = qps.process(dic1, dic2)
    print(dic3['dataProfile'])

    dic2 = dict()
    dic2['defaultEncodingMethod'] = 'onehot'
    ces = CategoryColumnEncodingService()
    dic3 = ces.process(dic1, dic2)
    print(dic3['modelOfEncoding'])


if __name__ == '__main__':
    main()
