#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ File  : BService.py
@ Author: LengWei
@ Date  : 2019-11-26
@ Desc  : 数据处理抽象类，作为数据处理接口

"""

import os
import fcntl
import time
import random
import datetime


class DServiceUtil():

    @staticmethod
    def initFileLock(path):
        if not os.path.exists(path):
            file = open(path, "w")
            file.close()

    @staticmethod
    def getFileLock(path):
        fp = open(path, "a+")
        fcntl.flock(fp, fcntl.LOCK_EX)
        return fp

    @staticmethod
    def releaseFileLock(fp):
        fcntl.flock(fp, fcntl.LOCK_UN)
        fp.close()


def main():
    # initFileLock('/home/user/lock.dat')

    for i in range(20):
        time.sleep(float(random.randint(0, 300)) / 300)
        f = DServiceUtil.getFileLock('/home/user/lock.dat')
        print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print(os.getpid(), "=>", i)
        time.sleep(10)
        DServiceUtil.releaseFileLock(f)


if __name__ == '__main__':
    main()
