#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: mark
# datetime: 2020/1/14 11:39
# software: PyCharm

import pandas as pd


class ValidationMethods(object):

    @staticmethod
    def validate_list_len(list1, list2, error_info=None):
        error_info = 'The two list length are not equal.' if not error_info else error_info
        if len(list1) == len(list2):
            return True
        else:
            raise ValueError(error_info)

    @staticmethod
    def validate_data_type(data, str_type, error_info=None):
        error_info = 'Parameter type must be {}.'.format(str_type) if not error_info else error_info
        if isinstance(data, eval(str_type)):
            return True
        else:
            raise TypeError(error_info)

    @staticmethod
    def validate_label_in_df(input_df_dict, input_control_dict, para_list, error_info=None):
        df = input_df_dict[para_list[0]]
        label = input_control_dict[para_list[1]]
        error_info = 'Given label: {} is not in columns of dataframe.'.format(label) if not error_info else error_info
        if label in df.columns:
            return True
        else:
            raise RuntimeError(error_info)
