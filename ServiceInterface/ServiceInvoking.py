#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: mark
# datetime: 2020/1/14 11:20
# software: PyCharm

import pandas as pd
import importlib

from ServiceInterface.ConstraintUtility import ValidationMethods as validations


def singleton(cls):
    instance = cls()
    instance.__call__ = lambda: instance
    return instance


@singleton
class ServiceInvoking(object):

    list_len_error = "The given input_df_list length must be {}"
    service_error = "The given service: {} is not in service list"

    def __init__(self):
        self.conf_df_input_dfs = pd.read_csv('../ConfigFiles/ServiceInputDfs.csv')
        self.conf_df_control_paras = pd.read_csv('../ConfigFiles/ServiceControlParas.csv')
        self.conf_df_constraints = pd.read_csv('../ConfigFiles/ServiceConstraints.csv')
        self.conf_df_output_dfs = pd.read_csv('../ConfigFiles/ServiceOutputParas.csv')

    def _df_to_dict(self, df, service_name):
        # get configuration dict
        conf_df = df[df['serviceName'] == service_name]
        conf_df.drop(['serviceName'], axis=1, inplace=True)
        conf_dict = df.to_dict(orient='list')
        return conf_dict

    def deal_with_input_dfs(self, service_name, input_df_list):
        # get configuration dict
        conf_dict = self._df_to_dict(self.conf_df_input_dfs, service_name)

        # validate the length of input_df_list
        validations.validate_list_len(conf_dict['inputPara'], input_df_list,
                                      self.list_len_error.format(str(len(conf_dict['inputPara']))))

        input_df_dict = dict()
        # validate the input df type
        for i in range(len(conf_dict['inputPara'])):
            if conf_dict['inputType'][i].lower() == 'dataframe':
                # validate input_df_list type
                validations.validate_data_type(input_df_list[i], 'pd.DataFrame')
            # TODO: There may be other validation methods to check, for instance 'mode type'
            # build input dataframe dict
            input_df_dict[conf_dict['inputPara'][i]] = input_df_list[i]

        # return input data dict
        return input_df_dict

    def deal_with_control_paras(self, service_name, specified_dict):
        # get configuration dict
        conf_dict = self._df_to_dict(self.conf_df_control_paras, service_name)

        # build default control parameters values
        para_list = conf_dict['paraName']
        type_list = conf_dict['paraType']
        value_list = conf_dict['defaultValue']
        para_len = len(para_list)
        default_value_dict = {para_list[i]: value_list[i] if type_list[i] == 'str' else eval(value_list[i]) for i in
                              range(para_len)}

        # check specified parameters types
        for para in specified_dict:
            index = conf_dict['paraName'].index(para)
            str_type = conf_dict['paraType'][index]
            # check whether the specified parameter is None.
            if specified_dict[para] is None or specified_dict[para].strip() == '':
                continue
            # invoke validation method.
            validations.validate_data_type(specified_dict[para], str_type)

            # cover default values with specified values
            default_value_dict[para] = specified_dict[para]

        # return control parameters
        return default_value_dict

    def deal_with_constraint_validation(self, service_name, input_df_dict, input_control_dict):
        # get configuration dict
        conf_dict = self._df_to_dict(self.conf_df_constraints, service_name)

        # loop every validation method
        method_list = conf_dict['constraintMethod']
        params_list = conf_dict['parameterList']
        for i in range(len(method_list)):
            # TODO: Specify keys in configuration file make the configuration too complex, it`s easy to make mistakes
            #  and hard to understand the logic, although the configuration files are fixed.
            #  For instance: modify the codes in service.
            # transform parameters to list.
            para_list = params_list[i].replace(' ', '').split(',')
            # invoke validation method
            validation_method = getattr(validations, method_list[i])
            validation_method(input_df_dict, input_control_dict, para_list)
            # TODO: need to implement other validation methods.

    def call_service_process(self, service_name, input_df_dict, input_control_dict):
        # invoke service using python reflection.
        c_service = importlib.import_module("ServiceImplByPandas.{}".format(service_name))
        if hasattr(c_service, service_name):
            service = getattr(c_service, service_name)
            # service object instantiation.
            service = service()
            # run the service.
            res = service.process(input_df_dict, input_control_dict)
        else:
            raise ValueError(self.service_error.format(service_name))
        return res

    def deal_with_output_paras(self, service_name, res):
        # get configuration dict
        conf_dict = self._df_to_dict(self.conf_df_output_dfs, service_name)
        output_list = []
        # sort the result by configuration.
        for i in range(len(conf_dict['outputPara'])):
            output_list.append(res[conf_dict['outputPara'][i]])
        return output_list

    @staticmethod
    def invoke_service(service_name, input_df_list, conf_dict):
        service = ServiceInvoking()
        # invoke input parameters process
        input_df_dict = service.deal_with_input_dfs(service_name, input_df_list)
        # invoke control parameters process
        input_control_dict = service.deal_with_control_paras(service_name, conf_dict)
        # invoke constraint methods
        service.deal_with_constraint_validation(service_name, input_df_dict, input_control_dict)
        # invoke service process method
        res = service.call_service_process(service_name, input_df_dict, input_control_dict)
        # deal with output parameters
        output_list = service.deal_with_output_paras(service_name, res)
        return output_list


if __name__ == "__main__":

    # Example code for knime:
    # from ServiceInterface.ServiceInvoking import ServiceInvoking


    input_table = pd.read_csv("E:/data/UCI_Credit_Card.csv")
    input_df_list = [input_table]
    # input_df_list=[input_table_1,input_table_2]
    # input_df_list = [input_table, input_model]

    input_control_dict = {
        'nameofLabelCol': 'target',
        'seedOfRandom': 0,
        'ratioOfPartition': 0.7
    }

    output_list = ServiceInvoking.invoke_service('RowPartitioningService', input_df_list, input_control_dict)

    output_table = output_list[0]
    # output_table_1=output_list[0]
    # output_table_2=output_list[1]
    # output_model=output_list[0]
