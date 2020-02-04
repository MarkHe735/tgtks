#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

from ServiceImplByPandas.EntityFeaturesService import EntityFeaturesService


def main():

    data = pd.read_csv(r"E:\data\UCI_Credit_Card.csv")
    print(data.dtypes)
    print(data.shape)

    dic1 = dict()
    dic1['data'] = data

    dic2 = dict()
    dic2['entityId'] = 'PAY_0'

    dic2['numericCols']     = ["LIMIT_BAL", "AGE"]
    dic2['categoryCols']    = ['SEX', 'EDUCATION', 'MARRIAGE', 'target']
    dic2['aggPrimitives']   = ["percent_true", "count", "mode", "mean", "sum", "max", "min"]
    dic2['transPrimitives'] = ["and", "or", "not"]
    dic2['wherePrimitives'] = ["count", "mode", "mean", "sum", "max", "min"]
    dic2['max_depth']       = 2

    ps = EntityFeaturesService()
    dic3 = ps.process(dic1, dic2)

    p1 = dic3['features']
    print(p1.dtypes)
    print(p1.shape)
    print(p1.columns)
    print(p1)


if __name__ == '__main__':
    main()
