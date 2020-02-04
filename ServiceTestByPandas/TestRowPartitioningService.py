#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

from ServiceImplByPandas.RowPartitioningService import RowPartitioningService


def main():

    data = pd.read_csv("../TestData/UCI_Credit_Card.csv")
    print(data.dtypes)
    print(data.shape)

    dic1 = dict()
    dic1['data'] = data

    dic2 = dict()
    dic2['nameofLabelCol'] = 'target'

    """
    dic2['seedOfRandom'] = 112
    dic2['flagOfShuffle'] = True
    dic2['ratioOfPartition'] = 0.5
    dic2['flagOfStratify'] = False
    """

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
