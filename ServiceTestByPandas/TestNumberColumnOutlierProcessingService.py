#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

from ServiceImplByPandas.NumberColumnDataProfilingService import NumberColumnDataProfilingService
from ServiceImplByPandas.ColumnPartitioningByTypeService import ColumnPartitioningByTypeService
from ServiceImplByPandas.NumberColumnOutlierProcessingService import NumberColumnOutlierProcessingService

def main():

    data = pd.read_csv("/home/user/UCI_Credit_Card.csv")

    dic1 = dict()
    dic1['data'] = data
    dic2 = dict()
    cps = ColumnPartitioningByTypeService()
    dic3 = cps.process(dic1, dic2)


    dic1=dict()
    dic1['dataOfNumberColumn']=dic3['partitionOfNumberColumns']
    dic2 = dict()
    ncdp=NumberColumnDataProfilingService()
    dic3=ncdp.process(dic1,dic2)
    print(dic3['dataProfile'])
    dataProfile=dic3['dataProfile']
    dataProfile.to_csv("/home/user/des.csv",index=False)


    dic1['dataProfile'] = dataProfile
    dic2 = dict()
    md=dict()
    md['LIMIT_BAL']='iqr'
    dic2['dictOfMethod']=md
    ncdp = NumberColumnOutlierProcessingService()
    dic3 = ncdp.process(dic1, dic2)
    print(dic3['dataWithoutOutlier'])

    dic3['dataWithoutOutlier'].to_csv("/home/user/out.csv",index=False)


if __name__ == '__main__':
    main()