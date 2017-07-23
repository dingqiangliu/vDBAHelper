#!/usr/bin/python
#encoding: utf-8
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: python code reflection
# Author: DingQiang Liu


import inspect
import types


def overridemodule(base, sub):
    """ get source code of overrided module 
    Arguments:
      base: Module
      sub: Module
    Return: source code of sub module which has been overrided by base module
    """

    # overite base to sub module
    for name, value in inspect.getmembers(base, lambda o: not inspect.isbuiltin(o)):
        if name in ['__builtins__', '__doc__', '__file__', '__name__', '__package__'] :
            continue
        if name not in sub.__dict__ : 
            sub.__dict__.update({name: value})


    src = ""
    # decompile module: imports
    # import from __future__ module first
    for name, value in inspect.getmembers(sub, lambda o: not inspect.isbuiltin(o) and (isinstance(o, types.ModuleType) or hasattr(o, '__module__') and not o.__module__ in [base.__name__, sub.__name__])):
        if hasattr(value, '__module__') and not value.__module__ in [base.__name__, sub.__name__] :
            if value.__module__ == '__future__' :
                src += "from %s import %s\n" %(value.__module__, name) 
    # import others
    for name, value in inspect.getmembers(sub, lambda o: not inspect.isbuiltin(o) and (isinstance(o, types.ModuleType) or hasattr(o, '__module__') and not o.__module__ in [base.__name__, sub.__name__])):
        if name in ['__builtins__', '__doc__', '__file__', '__name__', '__package__'] :
            continue
        if isinstance(value, types.ModuleType) :
            if value == base :
                src += "import sys\n"
                src += "%s = sys.modules[__name__] \n" % name 
            else :
                src += "import %s\n" % name 
        elif hasattr(value, '__module__') and not value.__module__ in [base.__name__, sub.__name__] :
            if value.__module__ != '__future__' :
                src += "from %s import %s\n" %(value.__module__, name) 
    src += "\n"

    # decompile module: constants
    for name, value in inspect.getmembers(sub, lambda o: not inspect.isbuiltin(o) and not isinstance(o, types.ModuleType) and not hasattr(o, '__module__')):
        if name in ['__builtins__', '__doc__', '__file__', '__name__', '__package__'] :
            continue
        src += "%s = %s\n" % (name, repr(value))
    src += "\n"

    # decompile module: classs and functions
    for name, value in inspect.getmembers(sub, lambda o: not inspect.isbuiltin(o) and hasattr(o, '__module__') and o.__module__ in [base.__name__, sub.__name__]):
        if name in ['__builtins__', '__doc__', '__file__', '__name__', '__package__'] :
            continue
        try :
            if hasattr(value, '__class__') and value.__class__.__name__ != "function" :
                # object/instance
                src += "%s = %s\n" % (name, repr(value))
            else :
                # function/class
                src += inspect.getsource(value) + "\n"
        except :
            raise

    # decompile module: non class/functions
    src += "\n".join([l for l in inspect.getsource(sub).split('\n') if not 'import' in l]) + "\n"
    return src
