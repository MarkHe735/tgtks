#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : BService.py
@ Author: LengWei
@ Date  : 2019-11-26
@ Desc  : 数据处理抽象类，作为数据处理接口

"""

import time
from abc import abstractmethod, ABCMeta
from IService.DServiceUtil import DServiceUtil
import pandas as pd
import uuid


class DService(metaclass=ABCMeta):

    def service(self, dicInput, dicParams):

        # TODO: Validate input & config

        container = dicParams.get('container')
        if container is None:
            container = '/home/user/tmp/tmp'

        dicInput1 = dict()
        fp = DServiceUtil.getFileLock(container + "/lock.dat")
        for key in dicInput:
            item = dicInput[key]
            name = item['name'][0]
            store = pd.HDFStore(container + '/' + name)
            dicInput1[key] = store['table']
            store.close()
        DServiceUtil.releaseFileLock(fp)

        start_time = time.time()
        dicOutput1 = self.process(dicInput1, dicParams)
        print('use time: %.3f s' % (time.time() - start_time))

        dicOutput = dict()
        fp = DServiceUtil.getFileLock(container + "/lock.dat")
        for key in dicOutput1:
            item = dicOutput1[key]
            name = str(uuid.uuid4().hex) + '.h5'
            store = pd.HDFStore(container + '/' + name)
            store['table'] = item
            store.close()
            item1 = pd.DataFrame({'name': [name]})
            dicOutput[key] = item1
            store.close()
        DServiceUtil.releaseFileLock(fp)

        return dicOutput

    @abstractmethod
    def process(self, dicInput, dicParams):
        """子类必须实现数据处理功能"""
        pass
