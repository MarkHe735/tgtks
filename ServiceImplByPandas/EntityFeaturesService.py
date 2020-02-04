#!/usr/bin/env python
# -*- coding: utf-8 -*-

import featuretools as ft

from IService.DService import DService


class EntityFeaturesService(DService):
    """
        功能        利用featuretools执行特征衍生(不含时间窗功能)

        输入        原始数据       数据帧类型      data

        控制        类别实体id     字符串类型       entityId
        控制        数值特征列     列表类型         numericCols        None
        控制        类别特征列     列表类型         categoryCols       None
        控制        聚合算法集合   列表类型         aggPrimitives      []
        控制        转换算法集合   列表类型         transPrimitives    []
        控制        where算法集合  列表类型         wherePrimitives    []
        控制        特征衍生深度   列表类型         max_depth          2

        输出        衍生的特征数据 数据帧类型       matrix

    """

    def process(self, dicInput, dicParams):
        """
        组件接口
        :param dicInput: 包含数据集的字典
        :param dicParams: 包含空值参数的字典
        :return: 结果数据字典
        """
        data = dicInput.get('data')
        if isinstance(data, type(None)):
            raise Exception("no input data")

        entity_id = dicParams.get('entityId')
        if entity_id is None:
            raise Exception('entityId could not be None.')

        numeric_cols = dicParams.get('numericCols')
        category_cols = dicParams.get('categoryCols')

        agg_primitives = dicParams.get('aggPrimitives')
        if agg_primitives is None:
            agg_primitives = []

        trans_primitives = dicParams.get('transPrimitives')
        if trans_primitives is None:
            trans_primitives = []

        where_primitives = dicParams.get('wherePrimitives')
        if where_primitives is None:
            where_primitives = []

        max_depth = dicParams.get('max_depth')
        if where_primitives is None:
            max_depth = 2

        features = self.__calculate_features(data, entity_id, numeric_cols, category_cols,
                                             agg_primitives, trans_primitives, where_primitives, max_depth)

        dicOutput = dict()
        dicOutput['features'] = features

        return dicOutput

    def __calculate_features(self, data, entity_id, numeric_cols, category_cols,
                             agg_primitives, trans_primitives, where_primitives, max_depth):
        """
        利用featuretools执行特征衍生。
        此方法计算一个实体的特征衍生。
        此方法未涉及时间窗特征衍生的计算。

        :param data: 原始数据                                           dataframe
        :param entity_id: 被计算实体的id                                str
        :param numeric_cols: 实体关联的数值型原始特征                   list
        :param category_cols: 实体关联的类别型原始特征                  list
        :param agg_primitives: 特征计算的聚合算法（原语）               list
        :param trans_primitives: 特征计算的转换算法（原语）             list
        :param where_primitives: 类别特征交叉衍生的where条件算法（原语）list
        :param max_depth: 特征衍生深度（默认值：2）                     list
        :return: 执行featuretools特征衍生后的结果                       dataframe
        """
        entity_df = data[[entity_id] + numeric_cols + category_cols]
        for col in category_cols:
            entity_df[col] = entity_df[col].astype('category')
        # Featuretools组织实体，实体间关系
        es = ft.EntitySet(id="entity_set")
        es = es.entity_from_dataframe(entity_id="original_dataset",
                                      dataframe=entity_df,
                                      index='_id',
                                      make_index=True)

        es = es.normalize_entity(base_entity_id="original_dataset",
                                 new_entity_id=entity_id,
                                 index=entity_id,
                                 make_time_index=False)

        if category_cols and isinstance(category_cols, list) and len(category_cols) > 0:
            # 设置where条件
            for col in category_cols:
                es["original_dataset"][col].interesting_values = data[col].unique().tolist()

            # 设置风险条件
            seed_list = [ft.Feature(es["original_dataset"][col]) == value
                         for col in category_cols for value in data[col].unique()]
        else:
            seed_list = None

        # 执行featuretools特征衍生
        matrix, _ = ft.dfs(entityset=es,
                           target_entity=entity_id,
                           agg_primitives=agg_primitives,
                           where_primitives=where_primitives,
                           trans_primitives=trans_primitives,
                           seed_features=seed_list,
                           max_depth=max_depth)

        return matrix
