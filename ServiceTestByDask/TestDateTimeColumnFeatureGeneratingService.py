#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ServiceImplByPandas.DateTimeColumnFeatureGeneratingService import DateTimeColumnFeatureGeneratingService
import pandas as pd

def main():

    data=pd.read_csv('../TestData/test.csv',parse_dates=['Date'])

    dic1 = dict()
    dic1['dataOfDateTimeColumn'] = data[['Date']]
    print(data.dtypes)

    dic2 = dict()

    cps = DateTimeColumnFeatureGeneratingService()
    dic3 = cps.process(dic1, dic2)
    print(dic3['dataOfDateTimeFeature'])



if __name__ == '__main__':
    main()