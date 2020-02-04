#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : DataMerge.py
@ Author: WangJun
@ Date  : 2019-11-26
@ Desc  : 多个数据帧拼接，生成宽表数据

"""
import pandas as pd
from IService.DService import DService

'''
idCol = 'id'
DateDf = pd.read_csv('/home/wj/code3/knimeModule/knime20191126/data/TrainData_datatime.csv')
ValueDf = pd.read_csv('/home/wj/code3/knimeModule/knime20191126/data/TrainData_numeric.csv')
ClassDf = pd.read_csv('/home/wj/code3/knimeModule/knime20191126/data/TrainData_class.csv')
LabelDf = pd.read_csv('/home/wj/code3/knimeModule/knime20191126/data/TrainData_label.csv')
'''
class DataMerge(DService):

    def __init__(self):
        pass
    def DataMergeAll(self,idCol, DateDf, ValueDf, ClassDf, LabelDf):
        '''
        :param idCol: id名称
        :param DateDf: 日期格式数据表
        :param ValueDf: 数值类型数据表
        :param ClassDf: 类别类型数据表
        :param LabelDf: 标签数据表
        :return: 以上各类数据合并后的数据表
        '''
        if len(LabelDf) > 0 and len(DateDf) > 0 :
            DataAll = LabelDf.merge(DateDf,on=idCol,how='left')
        elif len(LabelDf) > 0 and len(DateDf) == 0 :
            DataAll = LabelDf
        else :
            print('Missing LabelDf!')

        if len(ValueDf) > 0 :
            DataAll = DataAll.merge(ValueDf, on=idCol, how='left')
        else :
            DataAll = DataAll

        if len(ClassDf) > 0 :
            DataAll = DataAll.merge(ClassDf, on=idCol, how='left')
        else :
            DataAll = DataAll

        return DataAll



    def process(self, dicInput, dicParams):
        """
        :*功能*: 对数据帧进行数据探查. (N ==> 1)

        :param dicInput:  <dict> 输入的DataFrame数据表列表。形式:{'inputDf1':<DataFrame>, 'inputDf2':<DataFrame>,...}
        :param dicParams:  （类似yaml格式）
                - on : <dict> 必选。
                    { 列名：填充值} 的字典，填充值支持函数形式。
                - how:

        :*示例*:
            ::
               CP08:
                  mapping: {"MONTH_ON_BOOK":-1, "EDUCA":4, "OCC_CATGRY":5, "YR_IN_COMP":0}  # support functions
                  outfile: "BaseFeatureDerive_CP08"

        :return: dicOutput :  <dict> 输出的DataFrame数据表。形式: {'outputDf': <dataDf>}
        """
        dicOutput = dict()

        # 输入数据
        inputDf = dicInput['inputDf']
        DateDf = inputDf.get('inputDf1')
        ValueDf = inputDf.get('inputDf2')
        ClassDf = inputDf.get('inputDf3')
        LabelDf = inputDf.get('inputDf4')

        # 控制参数
        mapping = dicParams['mapping']
        idCol = mapping.get('idCol')


        # 数据处理
        DataAll = self.DataMergeAll(idCol, DateDf, ValueDf, ClassDf, LabelDf)
        # 输出
        #outputDf = inputDf
        dicOutput['outputDf1'] = DataAll

        return dicOutput