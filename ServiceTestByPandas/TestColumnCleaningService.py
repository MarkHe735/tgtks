#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
from ServiceImplByPandas.QualityProfilingService import QualityProfilingService
from ServiceImplByPandas.ColumnCleaningService import ColumnCleaningService
import pandas as pd

def main():
    data = pd.read_csv("/home/user/test.csv")
    dic1 = dict()
    dic1['data'] = data
    print(data.shape)

    dic2 = dict()

    qps = QualityProfilingService()
    dic3 = qps.process(dic1, dic2)
    dqp = dic3['qualityProfile']
    print(dqp)

    dic1 = dict()
    dic1['data'] = data
    dic1['qualityProfile'] = dqp
    dic2 = dict()

    dic2['thresholdOfMissing'] = 0.7
    dic2['thresholdOfIdLikeness'] = 0.5
    dic2['thresholdOfConstLikeness'] = 0.7

    dccs = ColumnCleaningService()
    dic3 = dccs.process(dic1, dic2)
    print(dic3['dataCleaned'])


if __name__ == '__main__':
    main()
