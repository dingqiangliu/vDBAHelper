#!/usr/bin/python
#encoding: utf-8

# base

#from __future__ import generators

import sys
from functools import partial, wraps
from os import SEEK_SET

intvar = 1
strvar = 'abc'
bvar = True
listvar = ['a', 1, [True, 1.2] ]

caption = "Hello"


def ftest() :
    return caption


def hello() :
    return caption


class Person():
    def __init__(self, value) :
        self.value = value
    
    def hello(self) :
        return "%s, %s" % (caption, self.value)


if __name__ == '__main__' :
    print hello()
else :
    per = Person('World')

