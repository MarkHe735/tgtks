#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd


class ColumnConcatingService(DService):
    """
    功能        列拼接

    输入        待过滤的数据     　数据帧类型      dataLeft
    输入        待过滤的数据     　数据帧类型      dataRight

    　
    输出        拼接后的数据       数据帧类型      dataConcated
    """

    def process(self, dicInput, dicParams):

        dataLeft = dicInput.get('dataLeft')
        if isinstance(dataLeft, type(None)):
            raise Exception("no input dataLeft")

        dataRight = dicInput.get('dataRight')
        if isinstance(dataRight, type(None)):
            raise Exception("no input dataRight")

        # TODO: 应统一做内部方法封装
        d1 = dataLeft.reset_index(drop=True).compute()
        d2 = dataRight.reset_index(drop=True).compute()

        list = []
        if (d1.shape[1] > 0):
            list.append(d1)
        if (d2.shape[1] > 0):
            list.append(d2)

        if (len(list) == 0):
            dataConcated = pd.DataFrame()
        else:
            dataConcated = pd.concat(list, axis=1)

        dicOutput = dict()
        dicOutput['dataConcated'] = dataConcated
        return dicOutput
