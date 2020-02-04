#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ServiceImplByPandas.ColumnPartitioningByTypeService import ColumnPartitioningByTypeService
import pandas as pd




def main():
    data = pd.read_csv("/home/user/UCI_Credit_Card.csv")
    dic1 = dict()
    dic1['data'] = data
    print(data.dtypes)

    dic2 = dict()

    cps = ColumnPartitioningByTypeService()
    dic3 = cps.process(dic1, dic2)
    print(dic3['partitionOfNumberColumns'].columns)
    print(dic3['partitionOfCategoryColumns'].columns)
    print(dic3['partitionOfDatetimeColumns'].columns)
    print(dic3['partitionOfOtherColumns'].columns)


if __name__ == '__main__':
    main()