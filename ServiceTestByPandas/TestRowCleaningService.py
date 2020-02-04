#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ServiceImplByPandas.RowCleaningService import RowCleaningService
import pandas as pd

def main():

    data = pd.read_csv("/home/user/test.csv")
    dic1=dict()
    dic1['data']=data

    dic2 = dict()
    dic2['thresholdOfMissingRatio']=0.02

    dccs = RowCleaningService()
    dic3=dccs.process(dic1,dic2)
    print(dic3['dataCleaned'])

if __name__ == '__main__':
    main()