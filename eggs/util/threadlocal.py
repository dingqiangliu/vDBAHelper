#!/usr/bin/python
#encoding: utf-8
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: Errors Navigation web controller
# Author: DingQiang Liu


import threading
threadlocal = threading.local()    


def threadlocal_set(name, value):
    setattr(threadlocal, name, value)


def threadlocal_get(name):
    return getattr(threadlocal, name, None)


def threadlocal_del(name):
    return delattr(threadlocal, name)
