#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
from xgboost import XGBClassifier
from sklearn.model_selection import GridSearchCV
import pandas as pd


class XGboostGridSearchingService(DService):
    """
    功能      XGBoost网格搜索建模

    　输入    训练集     　     数据帧类型      trainData
      控制    标签列名称  　    字符串类型      colnameOfLabelColumn     label
      控制    topK超参数组数    整数类型        k                        3

    输出      模型列表      　　数据帧类型      modelList

    """

    def process(self, dicInput, dicParams):

        trainData = dicInput.get('trainData')
        if isinstance(trainData, type(None)):
            raise Exception("no input trainData")

        if (trainData.shape[0] < 100):
            raise Exception("trainData row_number < 100")
        if (trainData.shape[1] == 0):
            raise Exception("trainData col_number = 0")

        colnameOfLabelColumn = dicParams.get('colnameOfLabelColumn')
        if colnameOfLabelColumn is None:
            colnameOfLabelColumn = 'label'

        # TODO: 应统一做内部方法封装
        k = dicParams.get('k')
        if k is None:
            k = 3

        d_train = trainData.copy()
        y_train = d_train.pop(colnameOfLabelColumn)
        X_train = d_train

        params_space = {'n_estimators': [50, 100, 150, 300, 500, 750, 1000],
                        'max_depth': [3, 4, 5, 6, 7, 8],
                        'min_child_weight': [1, 2, 3, 4, 5, 6],
                        'subsample': [0.6, 0.7, 0.8, 0.9],
                        'colsample_bytree': [0.6, 0.7, 0.8, 0.9],
                        'gamma': [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
                        'reg_alpha': [0.025, 0.05, 0.1, 0.3, 1, 2, 3],
                        'reg_lambda': [0.025, 0.05, 0.1, 0.3, 1, 2, 3],
                        'learning_rate': [0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.3]
                        }

        steps = [['n_estimators'],
                 ['max_depth', 'min_child_weight'],
                 ['subsample', 'colsample_bytree'],
                 ['gamma'],
                 ['reg_alpha', 'reg_lambda'],
                 ['learning_rate']
                 ]

        other_params = {'learning_rate': 0.1, 'n_estimators': 110, 'max_depth': 5, 'min_child_weight': 1, 'seed': 0,
                        'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}

        df_list = []

        seq = 0
        for step in steps:
            cv_params = {}
            for param in step:
                cv_params[param] = params_space[param]

            model = XGBClassifier(**other_params)
            optimized_GBM = GridSearchCV(estimator=model, param_grid=cv_params, scoring='roc_auc', cv=3, verbose=1,
                                         n_jobs=-1, return_train_score=True)
            optimized_GBM.fit(X_train, y_train)
            for param in step:
                other_params[param] = optimized_GBM.best_params_[param]

            line = other_params.copy()
            line['score'] = optimized_GBM.best_score_
            params_df = pd.DataFrame(line, index=[seq])
            df_list.append(params_df)
            seq = seq + 1

        paramsList = pd.concat(df_list, axis=0)
        paramsList = paramsList.drop_duplicates()
        paramsList = paramsList.nlargest(k, 'score')
        paramsList = paramsList.reset_index()

        modelList = []
        for i in range(paramsList.shape[0]):
            # other_params = {'learning_rate': 0.1, 'n_estimators': 110, 'max_depth': 5, 'min_child_weight': 1, 'seed': 0,
            #                 'subsample': 0.8, 'colsample_bytree': 0.8, 'gamma': 0, 'reg_alpha': 0, 'reg_lambda': 1}
            for key in other_params.keys():
                other_params[key] = paramsList[key][i]
            model = XGBClassifier(**other_params)
            eval_set = [(X_train, y_train)]
            model.fit(X_train, y_train, eval_metric="auc", eval_set=eval_set, early_stopping_rounds=20, verbose=True)
            modelList.append(model)

        dicOutput = dict()
        dicOutput['modelList'] = modelList

        return dicOutput


def main():
    data = pd.read_csv("/home/user/tmp/xgboost/UCI_Credit_Card.csv")
    data.pop('ID')

    dic1 = dict()
    dic1['trainData'] = data
    print(data.shape)

    dic2 = dict()
    dic2['colnameOfLabelColumn'] = 'target'

    qps = XGboostGridSearchingService()
    dic3 = qps.process(dic1, dic2)
    print(dic3['modelList'])


if __name__ == '__main__':
    main()
