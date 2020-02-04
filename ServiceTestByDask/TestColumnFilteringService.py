#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

from ServiceImplByPandas.ColumnFilteringService import ColumnFilteringService


def main():

    data = pd.read_csv("../TestData/UCI_Credit_Card.csv")
    print(data.dtypes)
    print(data.shape)

    dic1 = dict()
    dic1['data'] = data

    dic2 = dict()
    dic2['colsIncluded'] = ['ID', 'target']

    ps = ColumnFilteringService()
    dic3 = ps.process(dic1, dic2)

    p1 = dic3['dataWithColumnIncluded']
    p2 = dic3['dataWithoutColumnIncluded']
    print(p1)
    print(p1.shape)
    print(p2)
    print(p2.shape)


if __name__ == '__main__':
    main()
