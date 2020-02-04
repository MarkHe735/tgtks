#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from ServiceImplByPandas.XGboostHyperOptimizingService import XGboostHyperOptimizingService



def main():

    data = pd.read_csv("/home/user/tmp/xgboost/UCI_Credit_Card.csv")
    data.pop('ID')

    dic1 = dict()
    dic1['trainData'] = data
    print(data.shape)

    dic2 = dict()
    dic2['colnameOfLabelColumn']='target'

    qps = XGboostHyperOptimizingService()
    qps.process(dic1, dic2)



if __name__ == '__main__':
    main()