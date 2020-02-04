#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

from ServiceImplByPandas.QualityProfilingService import QualityProfilingService


def main():

    data = pd.read_csv("../TestData/UCI_Credit_Card.csv")
    dic1 = dict()
    dic1['data'] = data

    dic2 = dict()

    qps = QualityProfilingService()
    dic3 = qps.process(dic1, dic2)
    print(dic3['qualityProfile'])


if __name__ == '__main__':
    main()
