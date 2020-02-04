#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IService.DService import DService
from catboost import CatBoostClassifier
from sklearn.model_selection import cross_val_score
import optuna

global X_train
global y_train


def objective(trial):
    param = {
        'objective': trial.suggest_categorical('objective', ['Logloss', 'CrossEntropy']),

        'n_estimators': trial.suggest_int('n_estimators', 20, 500),
        'learning_rate': trial.suggest_loguniform('learning_rate', 1e-2, 1.0),

        'colsample_bylevel': trial.suggest_uniform('colsample_bylevel', 0.01, 0.1),
        'depth': trial.suggest_int('depth', 1, 12),

        'boosting_type': trial.suggest_categorical('boosting_type', ['Ordered', 'Plain']),
        'bootstrap_type': trial.suggest_categorical('bootstrap_type',
                                                    ['Bayesian', 'Bernoulli', 'MVS']),

        'random_strength': trial.suggest_int('random_strength', 1, 20),
        'one_hot_max_size': trial.suggest_int('one_hot_max_size', 0, 25),
        'l2_leaf_reg': trial.suggest_int('l2_leaf_reg', 1, 10)

    }

    if param['bootstrap_type'] == 'Bayesian':
        param['bagging_temperature'] = trial.suggest_uniform('bagging_temperature', 0, 10)
    elif param['bootstrap_type'] == 'Bernoulli':
        param['subsample'] = trial.suggest_uniform('subsample', 0.1, 1)

    model = CatBoostClassifier(**param)
    scores = cross_val_score(model, X_train, y_train, cv=4, scoring='roc_auc', n_jobs=-1)
    return scores.mean()


class CatBoostHyperOptimizingService(DService):
    """
     功能        catboost 优化建模

    　输入       　训练集     　        数据帧类型           trainData
      控制         标签列名称  　       字符串类型           colnameOfLabelColumn        label
      控制         迭代次数             整数类型             iter                        20
      控制         topK超参数组数       整数类型             k                           3

      输出        模型列表      　　    数据帧类型           modelList

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

        topk_trials = study.trials_dataframe().nlargest(3, 'value')
        topk_trials = topk_trials.reset_index()

        modelList = []
        for i in range(topk_trials.shape[0]):
            number = topk_trials['number'][i]
            ps = study.trials[number].params
            model = CatBoostClassifier(**ps)
            eval_set = [(X_train, y_train)]
            model.fit(X_train, y_train, eval_set=eval_set, early_stopping_rounds=20, verbose=True)
            modelList.append(model)

        dicOutput = dict()
        dicOutput['modelList'] = modelList

        return dicOutput
