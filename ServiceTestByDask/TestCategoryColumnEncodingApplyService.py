#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

from ServiceImplByPandas.CategoryColumnEncodingApplyService import CategoryColumnEncodingApplyService
from ServiceImplByPandas.CategoryColumnEncodingService import CategoryColumnEncodingService
from ServiceImplByPandas.RowPartitioningService import RowPartitioningService


def main():
    data = pd.read_csv("/home/user/UCI_Credit_Card.csv")

    dic1 = dict()
    dic1['data'] = data

    dic2 = dict()
    dic2['nameofLabelCol'] = 'target'
    dic2['seedOfRandom'] = 112
    dic2['flagOfShuffle'] = True
    dic2['ratioOfPartition'] = 0.8
    dic2['flagOfStratify'] = True

    ps = RowPartitioningService()
    dic3 = ps.process(dic1, dic2)
    p1 = dic3['partition1']
    p2 = dic3['partition2']

    p1 = p1[['EDUCATION']]
    p1 = p1.astype('str')

    p2 = p2[['EDUCATION']]
    p2 = p2.astype('str')

    dic1 = dict()
    dic1['dataOfCategoryColumn'] = p1
    dic2 = dict()
    dic2['defaultEncodingMethod'] = 'freq'
    ces = CategoryColumnEncodingService()
    dic3 = ces.process(dic1, dic2)

    dic1['dataOfCategoryColumn'] = p2
    dic1['modelOfEncoding'] = dic3['modelOfEncoding']
    ces = CategoryColumnEncodingApplyService()
    dic3 = ces.process(dic1, dic2)
    print(dic3['dataEncoded'])
    print(dic3['dataEncoded'].columns)


if __name__ == '__main__':
    main()
