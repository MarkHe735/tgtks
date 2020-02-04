#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd


class ColumnCleaningService(DService):
    """
     功能        清洗低质量数据列

    　输入        待清洗的数据     　数据帧类型      data
      输入        数据质量画像     　数据帧类型      qualityProfile
    　
      控制        缺失率阀值       　　浮点类型      thresholdOfMissing           0.8
      控制        标识似然度阀值       浮点类型      thresholdOfIdLikeness        0.8
      控制        常数似然度阀值       浮点类型      thresholdOfConstLikeness     0.8
      控制        缺失率字典       　　浮点类型      dictOfMissing               {}
      控制        标识似然度字典       浮点类型      dictOfIdLikeness            {}
      控制        常数似然度字典       浮点类型      dictOfConstLikeness         {}
    　
    　输出        清洗后的数据       数据帧类型      dataCleaned

    　注释：输入为空表，输出为空表
    """

    def process(self, dicInput, dicParams):

        data = dicInput.get('data')
        if isinstance(data, type(None)):
            raise Exception("no input data")

        qualityProfile = dicInput.get('qualityProfile')
        if isinstance(data, type(None)):
            raise Exception("no input qualityProfile")

        thresholdOfMissing = dicParams.get('thresholdOfMissing')
        if thresholdOfMissing is None:
            thresholdOfMissing = 0.8

        thresholdOfIdLikeness = dicParams.get('thresholdOfIdLikeness')
        if thresholdOfIdLikeness is None:
            thresholdOfIdLikeness = 0.8

        thresholdOfConstLikeness = dicParams.get('thresholdOfConstLikeness')
        if thresholdOfConstLikeness is None:
            thresholdOfConstLikeness = 0.8

        dictOfMissing = dicParams.get('dictOfMissing')
        if dictOfMissing is None:
            dictOfMissing = dict()

        dictOfIdLikeness = dicParams.get('dictOfIdLikeness')
        if dictOfIdLikeness is None:
            dictOfIdLikeness = dict()

        dictOfConstLikeness = dicParams.get('dictOfConstLikeness')
        if dictOfConstLikeness is None:
            dictOfConstLikeness = dict()

        if (data.shape[1] == 0):
            dataCleaned = pd.DataFrame()
        else:
            dataCleaned = self.__dataColumnCleaning(data, qualityProfile, dictOfConstLikeness,
                                                    dictOfIdLikeness, dictOfMissing, thresholdOfConstLikeness,
                                                    thresholdOfIdLikeness, thresholdOfMissing)

        dicOutput = dict()
        dicOutput['dataCleaned'] = dataCleaned

        return dicOutput

    def __dataColumnCleaning(self, data, qualityProfile, dictOfConstLikeness, dictOfIdLikeness,
                             dictOfMissing, thresholdOfConstLikeness, thresholdOfIdLikeness, thresholdOfMissing):
        """
        清洗低质量数据列
        :param data: 待清洗的数据
        :param qualityProfile: 数据质量画像
        :param dictOfConstLikeness: 常数似然度字典
        :param dictOfIdLikeness: 标识似然度字典
        :param dictOfMissing: 缺失率字典
        :param thresholdOfConstLikeness: 常数似然度阀值
        :param thresholdOfIdLikeness: 标识似然度阀值
        :param thresholdOfMissing: 缺失率阀值
        :return: 清洗后的数据
        """
        # 提取画像数据各个指数
        profileDict_missing = dict()
        profileDict_idLikeness = dict()
        profileDict_constLikeness = dict()
        for i in range(qualityProfile.shape[0]):
            col = qualityProfile.iloc[i][0]
            profileDict_missing[col] = qualityProfile.iloc[i][1]
            profileDict_idLikeness[col] = qualityProfile.iloc[i][2]
            profileDict_constLikeness[col] = qualityProfile.iloc[i][3]

        colsCleaned = []
        for col in data.columns:
            # 列缺失率是否在字典中给出
            thresholdOfMissing_col = dictOfMissing.get(col)
            if thresholdOfMissing_col is None:
                thresholdOfMissing_col = thresholdOfMissing
            # 列标识似然度是否在字典中给出
            thresholdOfIdLikeness_col = dictOfIdLikeness.get(col)
            if thresholdOfIdLikeness_col is None:
                thresholdOfIdLikeness_col = thresholdOfIdLikeness
            # 列常数似然度是否在字段中给出
            thresholdOfConstLikeness_col = dictOfConstLikeness.get(col)
            if thresholdOfConstLikeness_col is None:
                thresholdOfConstLikeness_col = thresholdOfConstLikeness

            if (profileDict_missing.get(col) >= thresholdOfMissing_col) or \
                    (profileDict_idLikeness.get(col) >= thresholdOfIdLikeness_col) or \
                    (profileDict_constLikeness.get(col) >= thresholdOfConstLikeness_col):
                pass
            else:
                # 保留各似然度均低于阈值的列
                colsCleaned.append(col)

        dataCleaned = data[colsCleaned].compute()
        return dataCleaned
