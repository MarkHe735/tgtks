#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd


class QualityProfilingService(DService):
    """
    功能        对数据质量进行画像

    输入        待质量画像的数据     数据帧类型      data
    　
    输出        数据质量画像      　 数据帧类型      dataQualityProfile

    注释：输入为空表，输出为空表

    """

    def process(self, dicInput, dicParams):

        data = dicInput.get('data')

        if (data.shape[1] == 0):
            qualityProfile = pd.DataFrame()
        else:
            qualityProfile = self.__qualityProfiling(data)

        dicOutput = dict()
        dicOutput['qualityProfile'] = qualityProfile

        return dicOutput

    def __qualityProfiling(self, data):
        """
        对数据质量进行画像
        :param data: 待质量画像的数据
        :return: 数据质量画像
        """
        col_list = []
        miss_list = []
        id_list = []
        const_list = []

        len = data.shape[0].compute()

        for col in data.columns:
            col_list.append(col)
            # 计算缺失率
            number_missing = len - data[col].count().compute()
            miss_list.append(number_missing * 1.0 / len)

            # 计算标识列比率
            # 默认浮点数和整数字段不是id字段
            value_counts = data[col].value_counts().compute()
            dd = data[col]
            type_col = str(dd.dtype)
            if 'float' in type_col or 'int' in type_col:
                id_list.append(0.0)
            else:
                id_list.append(value_counts.size * 1.0 / len)
            # 计算常数列比率
            const_list.append(value_counts.iloc[0] * 1.0 / len)

        qualityProfile = pd.DataFrame(
            {'col_name': col_list, 'missing_rate': miss_list, 'id_likeness': id_list, 'const_likeness': const_list})

        return qualityProfile
