#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

from ServiceImplByPandas.ToCategoryConvertingService import ToCategoryConvertingService


def main():
    data = pd.read_csv("/home/user/test.csv")
    dic1 = dict()
    dic1['data'] = data

    dic2 = dict()
    ncdp = ToCategoryConvertingService()
    dic3 = ncdp.process(dic1, dic2)
    print(dic3['dataOfCateorgyColumn'].head(10))
    print(dic3['dataOfCateorgyColumn'].dtypes)


if __name__ == '__main__':
    main()
