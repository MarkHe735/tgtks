#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd


class CSVDataReadingService(DService):
    """
           功能        读取CSV文件

           控制        文件路径         字符串类型     pathOfCsvFile
           控制        日期时间列列表   字符串列表     namelistOfDateTimeColumn     []
           控制        列头标志         布尔类型       flagOfColumnHeader           True

           输出        数据             数据帧类型     data

       """

    def process(self, dicInput, dicParams):

        pathOfCsvFile = dicParams.get('pathOfCsvFile')
        if pathOfCsvFile is None:
            raise Exception("no pathOfCsvFile")

        namelistOfDateTimeColumn = dicParams.get('namelistOfDateTimeColumn')
        if namelistOfDateTimeColumn is None:
            namelistOfDateTimeColumn = []

        flagOfColumnHeader = dicParams.get('flagOfColumnHeader')
        if flagOfColumnHeader is None:
            flagOfColumnHeader = True

        # TODO: 应统一做内部方法封装
        if flagOfColumnHeader:
            data = pd.read_csv(pathOfCsvFile, parse_dates=namelistOfDateTimeColumn)
        else:
            data = pd.read_csv(pathOfCsvFile, parse_dates=namelistOfDateTimeColumn, header=None)

        dicOutput = dict()
        dicOutput['data'] = data

        return dicOutput
