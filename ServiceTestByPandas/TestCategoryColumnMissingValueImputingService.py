#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd
from ServiceImplByPandas.LowCardColumnFilteringService import LowCardColumnFilteringService
from ServiceImplByPandas.NumberColumnDataProfilingService import NumberColumnDataProfilingService
from ServiceImplByPandas.ColumnPartitioningByTypeService import ColumnPartitioningByTypeService
from ServiceImplByPandas.ToCategoryConvertingService import ToCategoryConvertingService
from ServiceImplByPandas.CategoryColumnMissingValueImputingService import CategoryColumnMissingValueImputingService

def main():
    data = pd.read_csv("/home/user/test.csv")

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

    dic1['data']=dic1['dataOfNumberColumn']
    dic1['dataProfile'] = dic3['dataProfile']
    dic2 = dict()
    dic2['thresholdOfCard'] = 4
    lcnc = LowCardColumnFilteringService()
    dic3 = lcnc.process(dic1, dic2)

    dic1 = dict()
    dic1['data'] = dic3['dataOfLowCardColumn']
    dic2 = dict()
    ncdp = ToCategoryConvertingService()
    dic3 = ncdp.process(dic1, dic2)
    print(dic3['dataOfCateorgyColumn'])

    print("-------------------")

    dic1 = dict()
    dic1['dataOfCategoryColumn'] = dic3['dataOfCateorgyColumn']
    dic1['dataProfile'] = dataProfile
    dic2 = dict()
    md=dict()
    md['EDUCATION']='const_LOW'
    dic2['dictOfMethod']=md
    ncdp = CategoryColumnMissingValueImputingService()
    dic3 = ncdp.process(dic1, dic2)
    print(dic3['dataWithoutMissingValue'])


if __name__ == '__main__':
    main()