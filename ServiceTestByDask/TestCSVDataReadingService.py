#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ServiceImplByPandas.CSVDataReadingService import CSVDataReadingService
import pandas as pd




def main():

    dic1 = dict()
    dic2 = dict()
    dic2['pathOfCsvFile']='/home/user/loan_default-e.csv'

    cps = CSVDataReadingService()
    dic3 = cps.service(dic1, dic2)
    print(dic3['data'])


if __name__ == '__main__':
    main()