#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

from ServiceImplByPandas.CategoryColumnDataProfilingService import CategoryColumnDataProfilingService
from ServiceImplByPandas.NumberColumnDataProfilingService import NumberColumnDataProfilingService
from ServiceImplByPandas.LowCardColumnFilteringService import LowCardColumnFilteringService
from ServiceImplByPandas.NumberColumnBinningService import NumberColumnBinningService


def main():
    data = pd.read_csv("/home/user/UCI_Credit_Card.csv")
    dic1 = dict()
    dic1['dataOfNumberColumn'] = data

    dic2 = dict()
    ncdp = NumberColumnDataProfilingService()
    dic3 = ncdp.process(dic1, dic2)
    dp = dic3['dataProfile']
    print(dp)

    dic1['data'] = dic1['dataOfNumberColumn']
    dic1['dataProfile'] = dp
    dic2 = dict()
    dic2['thresholdOfCard'] = 20
    lcnc = LowCardColumnFilteringService()
    dic3 = lcnc.process(dic1, dic2)
    print(dic3['dataOfHighCardColumn'].columns)
    print(dic3['dataOfHighCardColumn'])

    dic1 = dict()
    dic1['dataOfNumberColumn'] = dic3['dataOfHighCardColumn']
    # TODO: 控制参数与组件接口定义不匹配
    dic2 = dict()
    dic2['defaultBinningMethod'] = 'EqualFreq'
    dic2['dictOfBinningMethod'] = {'PAY_AMT3': 'KMeans'}
    dic2['dictOfBinningNumber'] = {'PAY_AMT4': 100}
    ncdp = NumberColumnBinningService()
    dic3 = ncdp.process(dic1, dic2)
    print(dic3['modelOfBinning'])


if __name__ == '__main__':
    main()
