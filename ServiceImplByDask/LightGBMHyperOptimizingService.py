#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
import lightgbm as lgb
from sklearn.model_selection import cross_val_score
import optuna

global X_train
global y_train


def objective(trial):
    param = {
        'objective': 'binary',
        'metric': 'binary_logloss',
        'verbosity': -1,
        'boosting_type': 'gbdt',

        'n_estimators': trial.suggest_int('n_estimators', 20, 500),
        'learning_rate': trial.suggest_loguniform('learning_rate', 1e-2, 1.0),

        'num_leaves': trial.suggest_int('num_leaves', 5, 256),
        'max_depth': trial.suggest_int('max_depth', 3, 8),

        'reg_alpha': trial.suggest_loguniform('reg_alpha', 1e-8, 10.0),
        'reg_lambda': trial.suggest_loguniform('reg_lambda', 1e-8, 10.0),

        'min_data_in_leaf': trial.suggest_int('min_data_in_leaf', 5, 80),
        'max_bin': trial.suggest_int('max_bin', 5, 256),

        'feature_fraction': trial.suggest_uniform('feature_fraction', 0.4, 1.0),
        'bagging_fraction': trial.suggest_uniform('bagging_fraction', 0.4, 1.0),
        'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),

        'min_child_samples': trial.suggest_int('min_child_samples', 5, 100),
        'min_split_gain': trial.suggest_loguniform('reg_lambda', 1e-1, 1.0),
    }

    model = lgb.LGBMClassifier(**param)
    scores = cross_val_score(model, X_train, y_train, cv=4, scoring='roc_auc', n_jobs=-1)
    return scores.mean()


class LightGBMHyperOptimizingService(DService):
    """
     功能        LightGBM优化建模

    　输入       训练集     　      数据帧类型      trainData
      控制       标签列名称  　     字符串类型      colnameOfLabelColumn      label
      控制       迭代次数           整数类型        iter                      20
      控制       topK超参数组数     整数类型        k                         3

      输出       模型列表      　 　数据帧类型      modelList

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

        iter = dicParams.get('iter')
        if iter is None:
            iter = 20

        # TODO: 应统一做内部方法封装
        d_train = trainData.copy()
        y_train = d_train.pop(colnameOfLabelColumn)
        X_train = d_train

        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=iter)

        topk_trials = study.trials_dataframe().nlargest(3, 'value')
        topk_trials = topk_trials.reset_index()

        modelList = []
        for i in range(topk_trials.shape[0]):
            number = topk_trials['number'][i]
            ps = study.trials[number].params
            model = lgb.LGBMClassifier(**ps)
            eval_set = [(X_train, y_train)]
            model.fit(X_train, y_train, eval_metric="auc", eval_set=eval_set, early_stopping_rounds=20, verbose=True)
            modelList.append(model)

        dicOutput = dict()
        dicOutput['modelList'] = modelList

        return dicOutput
