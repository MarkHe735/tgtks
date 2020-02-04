#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import pandas as pd
from sklearn.model_selection import train_test_split


class RowPartitioningService(DService):
    """
        功能        按照比例拆分数据

        输入        待分割数据     数据帧类型      data

        控制        标签列         字符串类型       nameofLabelCol          label
        控制        混洗标志       布尔类型         flagOfShuffle           True        True：分区前混洗，Flase：分区前保持原样
        控制        分层标志       布尔类型         flagOfStratify          True        True：按照标签列分层取数（标签列必须是类别），Flase：随机取数
        控制        随机数种子     整型             seedOfRandom            None
        控制        分割比例       浮点类型         ratioOfPartition        0.7         0.0~1.0

        输出        数据分区1      数据帧类型      partition1
        输出        数据分区2      数据帧类型      partition2

    """

    def process(self, dicInput, dicParams):
        # 参数校验
        if isinstance(dicInput, dict):
            data = dicInput.get('data')
            if isinstance(data, pd.DataFrame):
                pass
            else:
                raise TypeError("The input data must be Dataframe, please check the type of your input parameter: data.")
        else:
            raise TypeError('The input parameter "dicInput" must be a dict.')

        if data.shape[0] < 100 or data.shape[1] == 0:
            raise ValueError("Your input data is empty or too small (less than 100 rows), shape of data: {}.".
                             format(str(data.shape)))

        if isinstance(dicParams, dict):
            nameofLabelCol = dicParams.get('nameofLabelCol')
            if nameofLabelCol is None:
                nameofLabelCol = 'label'
            if nameofLabelCol in data.columns:
                pass
            else:
                raise RuntimeError('The given label column is not in dataframe, label column: {}'.format(nameofLabelCol))

            flagOfShuffle = dicParams.get('flagOfShuffle')
            if flagOfShuffle is None:
                flagOfShuffle = True

            flagOfStratify = dicParams.get('flagOfStratify')
            if flagOfStratify is None:
                flagOfStratify = True

            seedOfRandom = dicParams.get('seedOfRandom')

            ratioOfPartition = dicParams.get('ratioOfPartition')
            if ratioOfPartition is None:
                ratioOfPartition = 0.7
        else:
            raise TypeError('The input parameter "dicParams" must be a dict.')

        # 调用处理方法
        partition1, partition2 = self.__Partitioning(data, nameofLabelCol, flagOfShuffle, flagOfStratify, seedOfRandom,
                                                     ratioOfPartition)
        # 输出返回值
        dicOutput = dict()
        dicOutput['partition1'] = partition1
        dicOutput['partition2'] = partition2

        return dicOutput

    def __Partitioning(self, data, nameofLabelCol, flagOfShuffle, flagOfStratify, seedOfRandom, ratioOfPartition):
        """
        按照比例拆分数据
        :param data: 待分割数据
        :param nameofLabelCol: 标签列名
        :param flagOfShuffle: 混洗标志
        :param flagOfStratify: 分层标志
        :param seedOfRandom: 随机数种子
        :param ratioOfPartition: 分割比例
        :return: 按照比例拆分后的两个数据集：partition1 partition2
        """
        # 分离标签列与数据集
        y = data.pop(nameofLabelCol)
        X = data

        if flagOfStratify:
            stratify_array = y
        else:
            stratify_array = None
        # 执行数据拆分
        X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=float(ratioOfPartition),
                                                            random_state=seedOfRandom, shuffle=flagOfShuffle,
                                                            stratify=stratify_array)
        # 将标签列合并回各数据集
        y_train_Df = pd.DataFrame({nameofLabelCol: y_train})
        y_test_Df = pd.DataFrame({nameofLabelCol: y_test})
        partition1 = pd.concat([X_train, y_train_Df], axis=1)
        partition2 = pd.concat([X_test, y_test_Df], axis=1)

        return partition1, partition2
