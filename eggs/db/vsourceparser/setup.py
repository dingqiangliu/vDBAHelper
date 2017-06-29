#!/usr/bin/env python
#encoding: utf-8
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: build script for vsourceparser module 
# Author: DingQiang Liu

from distutils.core import setup, Extension
from distutils.command.install import install
from shutil import copyfile
import os
import platform
from subprocess import call


class my_install(install):
    def run(self):
        myDir = os.path.dirname(os.path.realpath(__file__))
        
        print "copy file [%s/vsourceparser.so]" % self.build_lib, "to [%s/../vsourceparser.so.%s]" % (myDir, platform.system())
        copyfile("%s/vsourceparser.so" % self.build_lib, "%s/../vsourceparser.so.%s" % (myDir, platform.system()))
        call(["strip", ("-x" if platform.system()=="Darwin" else "") ,"%s/../vsourceparser.so.%s" % (myDir, platform.system())])
        
        print "copy file [%s/vsourceparser.py]" % myDir, "to [%s/../vsourceparser.py]" % myDir
        copyfile("%s/vsourceparser.py" % myDir, "%s/../vsourceparser.py" % myDir)


setup (
    name = 'vsourceparser',
    platforms = 'ALL',
    ext_modules = [Extension('vsourceparser', sources = ['vsourceparser.c'])],
    cmdclass=dict(install=my_install)
)