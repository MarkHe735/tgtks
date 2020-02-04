#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ServiceImplByPandas.ColumnConcatingService import ColumnConcatingService
import pandas as pd

def main():
    data = pd.read_csv("/home/user/test.csv")
    print(data)

    dic1 = dict()
    dic1['dataLeft'] = data
    dic1['dataRight'] = data
    dic2=dict()
    s=ColumnConcatingService()
    dic3=s.process(dic1,dic2)

    print(dic3['dataConcated'])
    print(dic3['dataConcated'].columns)


if __name__ == '__main__':
    main()