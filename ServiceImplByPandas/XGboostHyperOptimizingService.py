#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
from xgboost import XGBClassifier
from sklearn.model_selection import cross_val_score

# Optuna是一个自动超参数优化软件框架，专门为机器学习而设计。它具有命令式，运行式定义的用户API。
# 多亏了我们的运行定义API，用Optuna编写的代码具有高度的模块化，并且Optuna的用户可以动态构造超参数的搜索空间
import optuna

global X_train
global y_train


def objective(trial):
    param = {
        'silent': 1,
        'n_jobs': -1,

        'objective': 'binary:logistic',
        'eval_metric': 'auc',

        'booster': 'dart',

        'n_estimators': trial.suggest_int('n_estimators', 20, 500),
        'learning_rate': trial.suggest_loguniform('learning_rate', 1e-2, 1.0),

        'max_depth': trial.suggest_int('max_depth', 2, 10),
        'min_child_weight': trial.suggest_int('min_child_weight', 1, 20),

        'subsample': trial.suggest_loguniform('subsample', 5e-1, 1.0),
        'colsample_bytree': trial.suggest_loguniform('colsample_bytree', 5e-1, 1.0),
        'colsample_bylevel': trial.suggest_loguniform('colsample_bylevel', 5e-1, 1.0),
        'colsample_bynode': trial.suggest_loguniform('colsample_bynode', 5e-1, 1.0),

        'gamma': trial.suggest_loguniform('gamma', 1e-1, 5.0),
        'reg_lambda': trial.suggest_loguniform('reg_lambda', 3e-1, 1.0),
        'reg_alpha': trial.suggest_loguniform('reg_alpha', 3e-1, 1.0),

        'scale_pos_weight': trial.suggest_loguniform('scale_pos_weight', 1e-1, 5.0),
        'max_delta_step': trial.suggest_int('max_delta_step', 0, 5),

        'grow_policy': trial.suggest_categorical('grow_policy', ['depthwise', 'lossguide']),
        'sample_type': trial.suggest_categorical('sample_type', ['uniform', 'weighted']),
        'normalize_type': trial.suggest_categorical('normalize_type', ['tree', 'forest']),
        'rate_drop': trial.suggest_loguniform('rate_drop', 1e-1, 1.0),
        'skip_drop': trial.suggest_loguniform('skip_drop', 1e-1, 1.0)

    }

    model = XGBClassifier(**param)
    scores = cross_val_score(model, X_train, y_train, cv=4, scoring='roc_auc', n_jobs=-1)
    return scores.mean()


class XGboostHyperOptimizingService(DService):
    """
     功能        XGBoost贝叶斯优化建模

    　输入        训练集     　     数据帧类型      trainData
      控制        标签列名称  　    字符串类型      colnameOfLabelColumn        label
      控制        迭代次数          整数类型        iter                        20
      控制        topK超参数组数    整数类型        k                           3

      输出        模型列表      　　数据帧类型      modelList

    """

    def process(self, dicInput, dicParams):

        global X_train
        global y_train

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

        k = dicParams.get('k')
        if k is None:
            k = 3

        _iter = dicParams.get('iter')
        if _iter is None:
            _iter = 20

        # TODO: 应统一做内部方法封装
        d_train = trainData.copy()
        y_train = d_train.pop(colnameOfLabelColumn)
        X_train = d_train

        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=_iter)
        print(study.trials_dataframe())
        print(type(study.trials))
        print(len(study.trials))
        topk_trials = study.trials_dataframe().nlargest(3, 'value')
        topk_trials = topk_trials.reset_index()

        modelList = []
        for i in range(topk_trials.shape[0]):
            number = topk_trials['number'][i]
            ps = study.trials[number].params
            model = XGBClassifier(**ps)
            eval_set = [(X_train, y_train)]
            model.fit(X_train, y_train, eval_metric="auc", eval_set=eval_set, early_stopping_rounds=20, verbose=True)
            modelList.append(model)

        dicOutput = dict()
        dicOutput['modelList'] = modelList

        return dicOutput
